package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for normalizing natural-language prompts into Prompt Schema Workflows.
 *
 * <p>Uses LLM to convert user prompts into structured Prompt Schemas according to the Prompt Schema
 * Specification. The normalizer validates against a dictionary to ensure only canonical vocabulary
 * is used.
 */
public class PromptSchemaNormalizer {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(PromptSchemaNormalizer.class);

  private final OneMcp oneMcp;
  private final ObjectMapper objectMapper = JacksonUtility.getJsonMapper();

  public PromptSchemaNormalizer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Normalize a natural-language prompt into a Prompt Schema Workflow.
   *
   * @param prompt the user's natural-language prompt
   * @param dictionary the dictionary to validate against
   * @return normalized Prompt Schema Workflow
   * @throws ExecutionException if normalization fails
   */
  public PromptSchemaWorkflow normalize(String prompt, PromptDictionary dictionary)
      throws ExecutionException {
    if (prompt == null || prompt.trim().isEmpty()) {
      throw new IllegalArgumentException("Prompt cannot be null or empty");
    }
    if (dictionary == null) {
      throw new IllegalArgumentException("Dictionary cannot be null");
    }

    log.debug("Normalizing prompt: {}", prompt);

    // Validate dictionary has required fields
    if (dictionary == null) {
      throw new IllegalArgumentException("Dictionary cannot be null");
    }

    // Validate dictionary has required components
    if (dictionary.getActions() == null || dictionary.getActions().isEmpty()) {
      log.warn("Dictionary has no actions");
    }
    if (dictionary.getEntities() == null || dictionary.getEntities().isEmpty()) {
      log.warn("Dictionary has no entities");
    }
    if (dictionary.getFields() == null || dictionary.getFields().isEmpty()) {
      log.warn("Dictionary has no fields");
    }

    // Prepare prompt context
    Map<String, Object> context = new HashMap<>();
    context.put("user_prompt", prompt.trim());
    // Serialize dictionary to JSON string for better LLM consumption
    String dictionaryJson = serializeDictionaryToJson(dictionary);
    context.put("dictionary", dictionaryJson);

    log.debug(
        "Dictionary summary - Actions: {}, Entities: {}, Fields: {}, Operators: {}, Aggregates: {}",
        dictionary.getActions() != null ? dictionary.getActions().size() : 0,
        dictionary.getEntities() != null ? dictionary.getEntities().size() : 0,
        dictionary.getFields() != null ? dictionary.getFields().size() : 0,
        dictionary.getOperators() != null ? dictionary.getOperators().size() : 0,
        dictionary.getAggregates() != null ? dictionary.getAggregates().size() : 0);

    // Load and render normalization prompt
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("prompt-schema-normalize");
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("prompt-schema-normalize", context);

    List<LlmClient.Message> messages = session.renderMessages();
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from normalization prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    // Retry loop with validation feedback
    PromptSchemaWorkflow workflow = null;
    int maxAttempts = 3;
    int attempt = 0;
    List<String> previousErrors = new ArrayList<>();

    while (attempt < maxAttempts) {
      attempt++;

      // Add validation feedback from previous attempt if retrying
      List<LlmClient.Message> messagesToSend = new ArrayList<>(messages);
      if (attempt > 1 && !previousErrors.isEmpty()) {
        StringBuilder feedback = new StringBuilder();
        feedback.append("Your previous response was REJECTED due to validation errors:\n\n");
        for (String error : previousErrors) {
          feedback.append("❌ ").append(error).append("\n");
        }
        feedback.append(
            "\nCRITICAL: You MUST use ONLY values from the Dictionary provided above.\n");
        feedback.append("Please correct your response and try again.\n");
        feedback.append("Remember:\n");
        feedback.append("- Action MUST be in Dictionary.actions\n");
        feedback.append("- Entities MUST be in Dictionary.entities\n");
        feedback.append(
            "- Field names in params MUST be in Dictionary.fields (use underscores, not dots)\n");
        feedback.append("- Do NOT use 'query' unless it's in Dictionary.actions\n");

        messagesToSend.add(new LlmClient.Message(LlmClient.Role.USER, feedback.toString()));
        log.warn(
            "Retrying normalization (attempt {}/{}) with validation feedback",
            attempt,
            maxAttempts);
      }

      long inferenceStart = System.currentTimeMillis();

      // Create telemetry sink to capture token usage and set phase for logging
      final long[] tokenCounts = new long[3]; // [promptTokens, completionTokens, totalTokens]
      final Map<String, Object> sinkAttributes = new HashMap<>();
      sinkAttributes.put("phase", "normalize"); // Set phase so LLM client detects it correctly

      LlmClient.TelemetrySink tokenSink =
          new LlmClient.TelemetrySink() {
            @Override
            public void startChild(String name) {}

            @Override
            public void endCurrentOk(java.util.Map<String, Object> attrs) {}

            @Override
            public void endCurrentError(java.util.Map<String, Object> attrs) {}

            @Override
            public void addUsage(Long promptTokens, Long completionTokens, Long totalTokens) {
              if (promptTokens != null) tokenCounts[0] = promptTokens;
              if (completionTokens != null) tokenCounts[1] = completionTokens;
              if (totalTokens != null) tokenCounts[2] = totalTokens;
            }

            @Override
            public java.util.Map<String, Object> currentAttributes() {
              return sinkAttributes;
            }
          };

      String response;
      try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
        // LLM client will automatically log input messages and inference complete
        // with phase detected from telemetry sink attributes
        response = llmClient.chat(messagesToSend, Collections.emptyList(), false, null);
      }

      // Log token usage
      log.debug(
          "Token usage - prompt: {}, completion: {}, total: {}",
          tokenCounts[0],
          tokenCounts[1],
          tokenCounts[2]);

      // Extract JSON from response (may be wrapped in markdown code blocks)
      String jsonStr = extractJsonFromResponse(response);
      if (jsonStr == null || jsonStr.trim().isEmpty()) {
        log.error("No JSON content found in LLM response");
        log.debug("LLM response was: {}", response);
        if (attempt < maxAttempts) {
          previousErrors.clear();
          previousErrors.add("No JSON content found in response");
          continue;
        }
        throw new ExecutionException("No JSON content found in LLM response");
      }

      // Clean JSON: remove trailing commas and fix common issues
      jsonStr = cleanJson(jsonStr);

      // Log the extracted JSON for debugging
      log.debug(
          "Extracted JSON from LLM response (first 500 chars): {}",
          jsonStr.length() > 500 ? jsonStr.substring(0, 500) + "..." : jsonStr);

      // Parse JSON response
      try {
        workflow = objectMapper.readValue(jsonStr, PromptSchemaWorkflow.class);

        // Generate cache keys for all prompt schemas in the workflow
        if (workflow != null && workflow.getSteps() != null) {
          for (PromptSchemaStep step : workflow.getSteps()) {
            if (step != null && step.getPs() != null) {
              step.getPs().generateCacheKey();
              log.debug("Generated cache key for schema: {}", step.getPs().getCacheKey());
            }
          }
        }
      } catch (Exception parseError) {
        log.error(
            "Failed to parse Prompt Schema Workflow from LLM response (attempt {})",
            attempt,
            parseError);
        log.error(
            "Extracted JSON (first 1000 chars): {}",
            jsonStr != null && jsonStr.length() > 1000
                ? jsonStr.substring(0, 1000) + "..."
                : jsonStr);

        // Try to extract error location from exception message
        String errorLocation = null;
        if (parseError.getMessage() != null && parseError.getMessage().contains("column:")) {
          try {
            String msg = parseError.getMessage();
            int columnStart = msg.indexOf("column:");
            if (columnStart >= 0) {
              String columnPart = msg.substring(columnStart);
              int columnEnd = columnPart.indexOf(")");
              if (columnEnd > 0) {
                errorLocation = columnPart.substring(0, columnEnd + 1);
                // Show context around error
                try {
                  int columnNum = Integer.parseInt(columnPart.substring(8, columnEnd).trim());
                  int start = Math.max(0, columnNum - 100);
                  int end = Math.min(jsonStr.length(), columnNum + 100);
                  log.error(
                      "JSON context around error (column {}): ...{}...",
                      columnNum,
                      jsonStr.substring(start, end));
                } catch (NumberFormatException e) {
                  // Ignore
                }
              }
            }
          } catch (Exception e) {
            // Ignore
          }
        }

        // If we have retries left, retry with parse error feedback
        if (attempt < maxAttempts) {
          previousErrors.clear();

          // Provide specific feedback based on error type
          String errorMsg = parseError.getMessage();
          if (errorMsg != null) {
            if (errorMsg.contains("Unexpected")
                || errorMsg.contains("expected")
                || errorMsg.contains("close marker")) {
              previousErrors.add("❌ JSON SYNTAX ERROR: " + errorMsg);
              previousErrors.add("Your JSON has a syntax error. Common issues:");
              previousErrors.add(
                  "  - Trailing commas before closing brackets/braces (e.g., \"field\": \"value\", })");
              previousErrors.add("  - Mismatched brackets [ ] or braces { }");
              previousErrors.add("  - Missing quotes around string values");
              previousErrors.add("  - Invalid characters in JSON");
              previousErrors.add("");
              previousErrors.add("CRITICAL: The JSON structure must be:");
              previousErrors.add("  {");
              previousErrors.add("    \"workflow_type\": \"sequential\",");
              previousErrors.add("    \"steps\": [");
              previousErrors.add("      {");
              previousErrors.add("        \"ps\": {");
              previousErrors.add("          \"action\": \"summarize\",");
              previousErrors.add("          \"entities\": [\"sale\"],");
              previousErrors.add(
                  "          \"group_by\": [\"customer_state\"],  // OPTIONAL - NO trailing comma if last field");
              previousErrors.add("          \"params\": {");
              previousErrors.add(
                  "            \"sale_amount\": { \"aggregate\": \"sum\" }  // NO trailing comma");
              previousErrors.add("          }");
              previousErrors.add("        }");
              previousErrors.add("      }");
              previousErrors.add("    ]");
              previousErrors.add("  }");
              previousErrors.add("");
              previousErrors.add(
                  "IMPORTANT: Do NOT put a comma after the last item in arrays or objects!");
              // Try to find the problematic location
              if (errorMsg.contains("column:")) {
                try {
                  String columnPart = errorMsg.substring(errorMsg.indexOf("column:"));
                  int columnEnd = columnPart.indexOf(")");
                  if (columnEnd > 0) {
                    String colStr = columnPart.substring(7, columnEnd).trim();
                    int columnNum = Integer.parseInt(colStr);
                    int start = Math.max(0, columnNum - 30);
                    int end = Math.min(jsonStr.length(), columnNum + 30);
                    previousErrors.add("Error at column " + columnNum + ". Context:");
                    previousErrors.add("  ..." + jsonStr.substring(start, end) + "...");
                    previousErrors.add("  " + " ".repeat(Math.min(30, columnNum - start)) + "^");
                  }
                } catch (Exception e) {
                  // Ignore parsing errors
                }
              }
            } else if (errorMsg.contains("fields")) {
              previousErrors.add("JSON structure error: " + errorMsg);
              previousErrors.add("The 'fields' array must contain strings, not objects");
            } else {
              previousErrors.add("JSON parsing error: " + errorMsg);
            }
          } else {
            previousErrors.add("Failed to parse JSON - invalid structure");
          }

          // Add the actual JSON snippet to help debug (show more context around error)
          String jsonSnippet =
              jsonStr.length() > 1500 ? jsonStr.substring(0, 1500) + "..." : jsonStr;
          previousErrors.add("");
          previousErrors.add("Your JSON (first 1500 chars):");
          previousErrors.add(jsonSnippet);
          previousErrors.add("");
          previousErrors.add("Please fix the JSON syntax. Check for:");
          previousErrors.add("  1. Trailing commas (remove commas before } or ])");
          previousErrors.add(
              "  2. Mismatched brackets/braces (every { needs } and every [ needs ])");
          previousErrors.add(
              "  3. Proper string quoting (all string values must be in double quotes)");

          continue; // Retry with feedback
        }

        // If we've exhausted retries, throw
        throw new PromptNormalizationException(
            "Failed to parse Prompt Schema Workflow after "
                + maxAttempts
                + " attempts: "
                + parseError.getMessage()
                + ". Check the JSON syntax - ensure all brackets, braces, and commas are correct.",
            parseError,
            jsonStr);
      }

      // Validate the normalized workflow (but don't fail parsing if validation fails)
      List<String> validationErrors = new ArrayList<>();
      if (workflow != null) {
        validationErrors.addAll(workflow.validate(dictionary));

        // No additional validation needed - workflow.validate() already checks against dictionary

        if (!validationErrors.isEmpty()) {
          log.warn(
              "Normalized workflow has validation errors (attempt {}): {}",
              attempt,
              validationErrors);

          // If we have retries left, retry with feedback
          if (attempt < maxAttempts) {
            previousErrors = validationErrors;
            continue; // Retry with feedback
          }

          // If we've exhausted retries, log but still return (for now)
          log.error(
              "Validation failed after {} attempts. Errors: {}", maxAttempts, validationErrors);
          log.warn(
              "Returning workflow with validation errors - PSW is not used for execution, so continuing");
          return workflow; // Return even with validation errors - never throw for validation
          // failures
        }

        // Validation passed!
        log.debug(
            "Successfully normalized prompt to workflow with {} step(s) (attempt {})",
            workflow.getSteps().size(),
            attempt);
        return workflow;
      }
    }

    // This should never be reached, but handle null workflow case
    throw new ExecutionException(
        "Failed to normalize prompt: workflow is null after " + maxAttempts + " attempts");
  }

  /**
   * Serialize dictionary to JSON string for inclusion in the prompt.
   *
   * <p>JSON is preferred over YAML for LLMs because:
   *
   * <ul>
   *   <li>More explicit structure - less ambiguity
   *   <li>Better training data coverage - LLMs see more JSON
   *   <li>Easier to parse programmatically
   *   <li>More consistent formatting
   *   <li>Less prone to formatting errors
   * </ul>
   *
   * @param dictionary the dictionary to serialize
   * @return serialized dictionary as a JSON string (pretty-printed for readability)
   */
  private String serializeDictionaryToJson(PromptDictionary dictionary) {
    try {
      Map<String, Object> dictMap = new HashMap<>();

      // Ensure we always have lists (even if empty) to avoid null issues
      List<String> actions =
          dictionary.getActions() != null ? dictionary.getActions() : new ArrayList<>();
      List<String> entities =
          dictionary.getEntities() != null ? dictionary.getEntities() : new ArrayList<>();
      List<String> fields =
          dictionary.getFields() != null ? dictionary.getFields() : new ArrayList<>();
      List<String> operators =
          dictionary.getOperators() != null ? dictionary.getOperators() : new ArrayList<>();
      List<String> aggregates =
          dictionary.getAggregates() != null ? dictionary.getAggregates() : new ArrayList<>();

      // Log if critical arrays are empty
      if (actions.isEmpty()) {
        log.error("CRITICAL: Dictionary has NO actions! This will cause normalization to fail.");
      }
      if (entities.isEmpty()) {
        log.error("CRITICAL: Dictionary has NO entities! This will cause normalization to fail.");
      }
      if (fields.isEmpty()) {
        log.error("CRITICAL: Dictionary has NO fields! This will cause normalization to fail.");
      }

      dictMap.put("actions", actions);
      dictMap.put("entities", entities);
      dictMap.put("fields", fields);
      dictMap.put("operators", operators);
      dictMap.put("aggregates", aggregates);

      // Pretty-print JSON for better LLM readability
      String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dictMap);

      // Verify actions are in the JSON
      if (!json.contains("\"actions\"")) {
        log.error("Serialized JSON does not contain 'actions' key");
      } else if (json.contains("\"actions\" : [ ]")
          || json.contains("\"actions\" : []")
          || json.contains("\"actions\":[]")) {
        log.error("Serialized JSON has empty actions array");
      }

      return json;
    } catch (Exception e) {
      log.error("Failed to serialize dictionary to JSON", e);
      // Fallback to basic string representation
      return "{\"error\": \"Failed to serialize dictionary\", \"message\": \""
          + e.getMessage()
          + "\"}";
    }
  }

  /**
   * Extract JSON content from LLM response, handling markdown code blocks and other wrappers.
   *
   * @param response the raw LLM response
   * @return extracted JSON string, or null if not found
   */
  private String extractJsonFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String jsonStr = response.trim();

    // Remove markdown code block markers
    if (jsonStr.startsWith("```json")) {
      jsonStr = jsonStr.substring(7).trim();
    } else if (jsonStr.startsWith("```")) {
      jsonStr = jsonStr.substring(3).trim();
    }

    if (jsonStr.endsWith("```")) {
      jsonStr = jsonStr.substring(0, jsonStr.length() - 3).trim();
    }

    // Try to find JSON object boundaries if response contains extra text
    int firstBrace = jsonStr.indexOf('{');
    int lastBrace = jsonStr.lastIndexOf('}');

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      jsonStr = jsonStr.substring(firstBrace, lastBrace + 1);
    } else if (firstBrace >= 0) {
      // JSON starts but doesn't end properly - might be truncated
      jsonStr = jsonStr.substring(firstBrace);
    }

    return jsonStr.trim();
  }

  /**
   * Clean JSON string by removing trailing commas and fixing common syntax issues.
   *
   * @param json the JSON string to clean
   * @return cleaned JSON string
   */
  private String cleanJson(String json) {
    if (json == null || json.isEmpty()) {
      return json;
    }

    // Remove trailing commas before closing brackets/braces
    // Pattern: ,\s*[}\]]
    json = json.replaceAll(",\\s*([}\\]])", "$1");

    // Remove trailing commas in arrays/objects
    // Pattern: ,\s*] or ,\s*}
    json = json.replaceAll(",\\s*\\]", "]");
    json = json.replaceAll(",\\s*\\}", "}");

    // Remove extra closing brackets/braces at the end (common LLM error)
    // Find the last complete JSON object by matching braces
    json = removeTrailingExtraBrackets(json);

    return json.trim();
  }

  /**
   * Remove extra trailing brackets and braces that LLMs sometimes add. Finds the last complete JSON
   * object by matching braces from the end.
   *
   * @param json the JSON string to clean
   * @return cleaned JSON string with trailing extras removed
   */
  private String removeTrailingExtraBrackets(String json) {
    if (json == null || json.isEmpty()) {
      return json;
    }

    // Count braces and brackets from the end to find where the valid JSON ends
    int openBraces = 0;
    int openBrackets = 0;
    int validEnd = json.length();

    // Work backwards from the end
    for (int i = json.length() - 1; i >= 0; i--) {
      char c = json.charAt(i);
      if (c == '}') {
        openBraces++;
      } else if (c == '{') {
        openBraces--;
        if (openBraces == 0 && openBrackets == 0) {
          // Found the start of the root object - this is where valid JSON ends
          validEnd = i;
          break;
        }
      } else if (c == ']') {
        openBrackets++;
      } else if (c == '[') {
        openBrackets--;
      }

      // If we've closed all braces and brackets, we found the end
      if (openBraces == 0 && openBrackets == 0 && i < json.length() - 1) {
        validEnd = i + 1;
        // Check if there's more content after this - if so, it's likely extra
        String remaining = json.substring(validEnd).trim();
        if (remaining.matches("^[\\]\\}\\s]*$")) {
          // Only brackets/braces/whitespace remain - remove it
          return json.substring(0, validEnd);
        }
      }
    }

    // If we found a valid end point, use it
    if (validEnd < json.length()) {
      String trimmed = json.substring(0, validEnd).trim();
      String remaining = json.substring(validEnd).trim();
      // If remaining is just brackets/braces, remove it
      if (remaining.matches("^[\\]\\}\\s,]*$")) {
        return trimmed;
      }
    }

    return json;
  }
}
