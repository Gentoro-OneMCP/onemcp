package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizer for Prompt Schema (PS) - Uses flat conceptual lexicon.
 * 
 * <p>Converts natural-language prompts into structured JSON Prompt Schemas
 * using a flat vocabulary of actions, entities, and fields.
 */
public class PromptSchemaNormalizer {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(PromptSchemaNormalizer.class);

  private final OneMcp oneMcp;
  private final PromptSchemaCanonicalizer canonicalizer;

  public PromptSchemaNormalizer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    this.canonicalizer = new PromptSchemaCanonicalizer();
  }

  /**
   * Normalize a natural-language prompt into Prompt Schema (PS).
   *
   * @param prompt the user's natural-language prompt
   * @param lexicon the conceptual lexicon to use
   * @return Prompt Schema
   * @throws ExecutionException if normalization fails after all retries
   */
  public PromptSchema normalize(String prompt, ConceptualLexicon lexicon) throws ExecutionException {
    NormalizationResult result = normalizeWithRetry(prompt, lexicon, 3); // Max 3 attempts
    
    // Check if normalization failed (action is null)
    if (result.ps == null || result.ps.getAction() == null || result.ps.getAction().trim().isEmpty()) {
      // Normalization failed - use the PS from result if available, otherwise create new one
      PromptSchema ps = result.ps;
      if (ps == null) {
        ps = new PromptSchema();
        ps.setAction(null);
        ps.setEntity(null);
        ps.setNote(result.note != null ? result.note : "Normalization failed");
      }
      ps.setOriginalPrompt(prompt);
      ps.setCacheKey(generateFailureCacheKey(prompt));
      return ps;
    }
    
    // Set original prompt on the PS
    result.ps.setOriginalPrompt(prompt);
    
    // Canonicalize the PS
    PromptSchema canonical = canonicalizer.canonicalize(result.ps);
    
    // Set original prompt on canonicalized version (not part of canonicalization)
    canonical.setOriginalPrompt(prompt);
    
    // Generate cache key from canonical structure
    String cacheKey = canonicalizer.generateCacheKey(canonical);
    canonical.setCacheKey(cacheKey);
    
    return canonical;
  }

  /**
   * Normalize with retry logic and error feedback.
   */
  private NormalizationResult normalizeWithRetry(String prompt, ConceptualLexicon lexicon, int maxAttempts) 
      throws ExecutionException {
    int attempt = 0;
    List<String> previousErrors = new ArrayList<>();

    while (attempt < maxAttempts) {
      attempt++;
      log.debug("Starting normalization attempt {} of {}", attempt, maxAttempts);
      
      // Begin phase for this normalization attempt
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logPhaseBegin("normalize", attempt);
      }
      
      PromptSchema lastPs = null;  // Keep track of last PS for fix attempts
      String lastResponse = null;      // Keep track of last raw response for JSON parsing errors
      
      try {
        // Always pass previousErrors if available (even on first attempt, in case we want to log it)
        // But only include it in the input JSON if attempt > 1
        NormalizeResultWithResponse resultWithResponse = normalizeOnceWithResponse(prompt, lexicon, attempt > 1 ? previousErrors : null, attempt);
        NormalizationResult result = resultWithResponse.result;
        lastResponse = resultWithResponse.rawResponse;
        lastPs = result.ps;
        
        log.debug("Normalization attempt {} completed, got PS: action={}, entity={}", 
            attempt, result.ps != null ? result.ps.getAction() : "null", 
            result.ps != null ? result.ps.getEntity() : "null");
        
        // Validate result
        List<String> validationErrors = validatePromptSchema(result.ps, lexicon);
        
        // Compute phase name with attempt number for logging (e.g., "normalize#2")
        String phaseName = attempt > 1 ? "normalize#" + attempt : "normalize";
        
        if (validationErrors.isEmpty()) {
          if (result.ps != null) {
            log.info("Successfully normalized prompt to PS (attempt {}): action={}, entity={}", 
                attempt, result.ps.getAction(), result.ps.getEntity());
          }
          // End phase on success
          if (oneMcp.inferenceLogger() != null) {
            oneMcp.inferenceLogger().logPhaseEnd("normalize", attempt);
          }
          return result;
        }
        
        // LLM returned rejectable prompt (action is null but note is set)
        if (result.ps != null && result.ps.getAction() == null && result.ps.getNote() != null) {
          log.info("Prompt rejected by LLM: {}", result.ps.getNote());
          // End phase on rejection
          if (oneMcp.inferenceLogger() != null) {
            oneMcp.inferenceLogger().logPhaseEnd("normalize", attempt);
          }
          return result;
        }
        
        // Validation failed - try fast fix for simple validation errors
        String validationErrorMsg = String.join("; ", validationErrors);
        log.warn("VALIDATION FAILED on attempt {}: {}", attempt, validationErrorMsg);
        
        // Build the full error message FIRST (before any fast fix attempts)
        String basePhaseName = "normalize"; // Always use base phase for error logging
        String fullErrorMsg = "Validation failed: " + validationErrorMsg;
        
        // Add the error to previousErrors IMMEDIATELY (before fast fix attempts)
        // This ensures the error is available for the next attempt even if fast fix is tried
        previousErrors.add(fullErrorMsg);
        log.info("Added error to previousErrors list (now {} errors): {}", previousErrors.size(), fullErrorMsg);
        
        // Log validation error to inference logger for report visibility
        log.info("About to log validation error: phase={}, attempt={}, error={}", basePhaseName, attempt, fullErrorMsg);
        if (oneMcp.inferenceLogger() != null) {
          oneMcp.inferenceLogger().logValidationError(basePhaseName, attempt, fullErrorMsg);
          log.info("Validation error logged successfully to inference logger");
        } else {
          log.warn("Cannot log validation error: inference logger is null");
        }
        
        // Try fast fix for validation errors (only on first attempt, and only for simple structural errors)
        if (attempt == 1 && isSimpleValidationError(validationErrors) && lastPs != null) {
          log.info("Validation errors detected - attempting fast fix routine");
          try {
            NormalizationResult fixedResult = fixValidationErrors(lastPs, validationErrors, lexicon, "normalize");
            List<String> fixedErrors = validatePromptSchema(fixedResult.ps, lexicon);
            if (fixedErrors.isEmpty()) {
              log.info("Fast fix succeeded for validation errors - returning fixed PS");
              // Note: error was already added to previousErrors above, but we're returning success
              // so it won't be used. This is intentional for logging purposes.
              // End phase on success
              if (oneMcp.inferenceLogger() != null) {
                oneMcp.inferenceLogger().logPhaseEnd("normalize", attempt);
              }
              return fixedResult;
            } else {
              log.info("Fast fix did not resolve all errors (remaining: {}), will retry with full normalization", fixedErrors);
            }
          } catch (Exception fixException) {
            log.warn("Fast fix failed, will retry with full normalization: {}", fixException.getMessage());
          }
        }
        // previousErrors already has the error added above, ready for next attempt
        // End phase before retry
        if (oneMcp.inferenceLogger() != null) {
          oneMcp.inferenceLogger().logPhaseEnd("normalize", attempt);
        }
        
      } catch (Exception e) {
        String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        log.warn("EXCEPTION during normalization attempt {}: {}", attempt, errorMsg, e);
        
        // Build the full error message FIRST (before any fast fix attempts)
        String basePhaseName = "normalize"; // Always use base phase for error logging
        String fullErrorMsg = "Exception during normalization: " + errorMsg;
        
        // Add the error to previousErrors IMMEDIATELY (before fast fix attempts)
        previousErrors.add(fullErrorMsg);
        log.info("Added exception error to previousErrors list (now {} errors): {}", previousErrors.size(), fullErrorMsg);
        
        // Log exception error to inference logger for report visibility
        log.info("About to log exception error: phase={}, attempt={}, error={}", basePhaseName, attempt, fullErrorMsg);
        if (oneMcp.inferenceLogger() != null) {
          oneMcp.inferenceLogger().logValidationError(basePhaseName, attempt, fullErrorMsg);
          log.info("Exception error logged successfully to inference logger");
        } else {
          log.warn("Cannot log exception error: inference logger is null");
        }
        
        // End phase before retry
        if (oneMcp.inferenceLogger() != null) {
          oneMcp.inferenceLogger().logPhaseEnd("normalize", attempt);
        }
        
        // Check if this is a JSON parsing error that we can try to fix
        boolean isJsonParsingError = errorMsg.contains("Failed to parse") || 
                                     errorMsg.contains("JSON") ||
                                     errorMsg.contains("Unexpected token");
        
        // Try fast fix for JSON parsing errors (only on first attempt)
        if (attempt == 1 && isJsonParsingError && lastResponse != null) {
          log.info("JSON parsing error detected - attempting fast fix routine");
          try {
            // Try to extract and fix the JSON from the malformed response
            NormalizationResult fixedResult = fixJsonParsingError(lastResponse, errorMsg, lexicon, "normalize");
            List<String> fixedErrors = validatePromptSchema(fixedResult.ps, lexicon);
            if (fixedErrors.isEmpty()) {
              log.info("Fast fix succeeded for JSON parsing error - returning fixed PS");
              // Note: error was already added to previousErrors above, but we're returning success
              return fixedResult;
            } else {
              log.info("Fast fix did not resolve all errors (remaining: {}), will retry with full normalization", fixedErrors);
            }
          } catch (Exception fixException) {
            log.warn("Fast fix failed for JSON parsing error, will retry with full normalization: {}", fixException.getMessage());
          }
        }
        // previousErrors already has the error added above, ready for next attempt
      }
    }

    // All attempts failed
    throw new ExecutionException("Normalization failed after " + maxAttempts + " attempts. Errors: " + previousErrors);
  }

  /**
   * Perform a single normalization attempt.
   * @return NormalizationResult with the parsed PS, or throws ExecutionException if parsing fails
   */
  private NormalizationResult normalizeOnce(String prompt, ConceptualLexicon lexicon, List<String> previousErrors, int attempt)
      throws ExecutionException {
    return normalizeOnceWithResponse(prompt, lexicon, previousErrors, attempt).result;
  }
  
  /**
   * Perform a single normalization attempt, returning both result and raw response.
   */
  private static class NormalizeResultWithResponse {
    final NormalizationResult result;
    final String rawResponse;
    
    NormalizeResultWithResponse(NormalizationResult result, String rawResponse) {
      this.result = result;
      this.rawResponse = rawResponse;
    }
  }
  
  private NormalizeResultWithResponse normalizeOnceWithResponse(String prompt, ConceptualLexicon lexicon, List<String> previousErrors, int attempt)
      throws ExecutionException {
    
    // Build input JSON with flat lexicon
    Map<String, Object> inputData = new HashMap<>();
    inputData.put("prompt", prompt.trim());
    inputData.put("actions", lexicon.getActions());
    inputData.put("entities", lexicon.getEntities());
    inputData.put("fields", lexicon.getFields());
    
    // Add current_time for computing relative dates like "last quarter", "last month", etc.
    // Use ISO 8601 format (e.g., "2024-12-06T08:33:03.237743Z")
    java.time.Instant now = java.time.Instant.now();
    inputData.put("current_time", now.toString());
    
    // Add error feedback if retrying
    if (previousErrors != null && !previousErrors.isEmpty()) {
      String errorFeedback = String.join("\n", previousErrors);
      inputData.put("error_feedback", errorFeedback);
      log.info("Including error feedback in normalize attempt {}: {}", attempt, 
          errorFeedback.length() > 200 ? errorFeedback.substring(0, 200) + "..." : errorFeedback);
    } else {
      // Don't include error_feedback at all for first attempt (keep input consistent)
      log.info("No error feedback for normalize attempt {} (previousErrors={}, isEmpty={})", 
          attempt, previousErrors != null ? "non-null" : "null", 
          previousErrors != null ? previousErrors.isEmpty() : "N/A");
    }
    
    String inputJson;
    try {
      inputJson = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(inputData);
    } catch (Exception e) {
      throw new ExecutionException("Failed to serialize input data: " + e.getMessage(), e);
    }

    // Get prompt template
    PromptRepository promptRepository = oneMcp.promptRepository();
    PromptTemplate template = promptRepository.get("ps-normalize");
    if (template == null) {
      throw new ExecutionException("Prompt template 'ps-normalize' not found");
    }

    // Build messages from template using session
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("ps-normalize", Map.of("input_json", inputJson));
    List<LlmClient.Message> messages = session.renderMessages();
    
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered for 'ps-normalize'");
    }

    // Use phase name with attempt number for retries (e.g., "normalize#2")
    String phaseName = attempt > 1 ? "normalize#" + attempt : "normalize";
    long startTime = System.currentTimeMillis();
    
    // Note: Input messages and inference completion are logged by AbstractLlmClient
    // We just need to ensure the phase is set correctly in telemetry

    // Get temperature from template
    Float temperature = template.temperature().orElse(0.0f);

    // Call LLM with telemetry to set phase
    LlmClient llmClient = oneMcp.llmClient();
    
    // Create telemetry sink to capture tokens and set phase
    final long[] promptTokens = {0};
    final long[] completionTokens = {0};
    // Maintain attributes map so phase persists across multiple currentAttributes() calls
    final Map<String, Object> telemetryAttrs = new HashMap<>();
    telemetryAttrs.put("phase", phaseName);
    
    LlmClient.TelemetrySink telemetrySink = new LlmClient.TelemetrySink() {
      @Override
      public void startChild(String name) {
        // Log inference start when LLM client calls startChild (e.g., "llm.anthropic")
        // Only log once per normalization attempt, not for every child span
        if (name != null && name.startsWith("llm.") && oneMcp.inferenceLogger() != null) {
          oneMcp.inferenceLogger().logLlmInferenceStart(phaseName);
        }
      }
      
      @Override
      public void endCurrentOk(java.util.Map<String, Object> attrs) {
        // Clean the response if it's stored in attributes (removes Optional wrapper)
        // Note: attrs might be immutable, so we can't modify it directly
        // The response cleaning is handled in cleanResponse() which is called on the response string itself
        // before it's logged, so we don't need to modify attrs here
      }
      
      @Override
      public void endCurrentError(java.util.Map<String, Object> attrs) {}
      
      @Override
      public void addUsage(Long promptT, Long completionT, Long totalT) {
        if (promptT != null) promptTokens[0] = promptT;
        if (completionT != null) completionTokens[0] = completionT;
      }
      
      @Override
      public java.util.Map<String, Object> currentAttributes() {
        // Return the same mutable map so phase persists
        return telemetryAttrs;
      }
    };
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(telemetrySink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, temperature);
    } catch (Exception e) {
      throw new ExecutionException("LLM call failed: " + e.getMessage(), e);
    }
    
    long duration = System.currentTimeMillis() - startTime;
    
    // Note: Inference completion is logged by AbstractLlmClient.logInferenceComplete()
    // which detects the phase from telemetry attributes. No need to log here.
    
    if (response == null || response.trim().isEmpty()) {
      throw new ExecutionException("LLM returned empty response");
    }

    // Clean the response to remove Optional wrapper before any further processing
    // This ensures the cleaned response is what gets logged/displayed
    response = cleanResponse(response);

    // Extract JSON from response (may be wrapped in markdown code block)
    String jsonContent = extractJsonFromResponse(response);
    
    // Parse the response into PromptSchema
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      PromptSchema ps = mapper.readValue(jsonContent, PromptSchema.class);
      
      // Capture any note from the LLM
      String note = ps.getNote();
      
      // Parse successful - no need to log separately
      
      return new NormalizeResultWithResponse(new NormalizationResult(ps, note), response);
    } catch (Exception e) {
      // This is a JSON parsing error, not a validation error
      String jsonPreview = jsonContent != null && !jsonContent.isEmpty() 
          ? (jsonContent.length() > 500 ? jsonContent.substring(0, 500) + "..." : jsonContent)
          : "[empty or null]";
      String responsePreview = response != null && !response.isEmpty()
          ? (response.length() > 500 ? response.substring(0, 500) + "..." : response)
          : "[empty or null]";
      String errorMsg = "Failed to parse LLM response as JSON: " + e.getMessage() + 
          "\nExtracted JSON preview: " + jsonPreview +
          "\nOriginal response preview: " + responsePreview;
      
      // Parse failure - error will be logged via exception handling
      
      throw new ExecutionException(errorMsg, e);
    }
  }

  /**
   * Clean the response by removing Optional wrapper if present.
   * This should be called immediately after getting the response from the LLM.
   */
  private String cleanResponse(String response) {
    if (response == null) {
      return response;
    }
    String trimmed = response.trim();
    
    // Remove Optional[...] wrapper if present (handles Optional[```json ... ```] format)
    // "Optional[" is 9 characters (indices 0-8), so the '[' is at index 8 and content starts at index 9
    if (trimmed.startsWith("Optional[")) {
      int endBracket = findMatchingBracket(trimmed, 8); // '[' is at index 8
      if (endBracket > 0) {
        // Extract content INSIDE the brackets: skip '[' at index 8, end before ']' at endBracket
        trimmed = trimmed.substring(9, endBracket).trim();
        log.debug("Removed Optional[] wrapper, extracted content: {}", 
            trimmed.length() > 100 ? trimmed.substring(0, 100) + "..." : trimmed);
      }
    }
    
    return trimmed;
  }

  /**
   * Extract JSON from LLM response (handles markdown code blocks).
   * Note: Optional wrapper should already be removed by cleanResponse().
   */
  private String extractJsonFromResponse(String response) {
    String trimmed = response.trim();
    
    // Check for markdown JSON code block
    Pattern jsonBlock = Pattern.compile("```(?:json)?\\s*\\n?(.*?)\\n?```", Pattern.DOTALL);
    Matcher matcher = jsonBlock.matcher(trimmed);
    if (matcher.find()) {
      return matcher.group(1).trim();
    }
    
    // Check for just a code block without json tag
    Pattern codeBlock = Pattern.compile("```\\s*\\n?(.*?)\\n?```", Pattern.DOTALL);
    matcher = codeBlock.matcher(trimmed);
    if (matcher.find()) {
      return matcher.group(1).trim();
    }
    
    // Assume raw JSON
    return trimmed;
  }
  
  /**
   * Find the matching closing bracket for an opening bracket at the given position.
   * Handles nested brackets correctly.
   */
  private int findMatchingBracket(String str, int startPos) {
    if (startPos >= str.length() || str.charAt(startPos) != '[') {
      return -1;
    }
    int depth = 1;
    for (int i = startPos + 1; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '[') {
        depth++;
      } else if (c == ']') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return -1; // No matching bracket found
  }

  /**
   * Validate the Prompt Schema against the lexicon.
   */
  private List<String> validatePromptSchema(PromptSchema ps, ConceptualLexicon lexicon) {
    List<String> errors = new ArrayList<>();
    
    if (ps == null) {
      errors.add("PS is null");
      return errors;
    }
    
    // Rejection is valid
    if (ps.getAction() == null && ps.getNote() != null) {
      return errors; // Empty = valid rejection
    }
    
    // Validate action
    String action = ps.getAction();
    if (action == null || action.trim().isEmpty()) {
      errors.add("Action is required");
    } else if (!lexicon.getActions().contains(action.toLowerCase())) {
      errors.add("Unknown action: " + action + ". Valid: " + lexicon.getActions());
    }
    
    // Validate entity
    String entity = ps.getEntity();
    if (entity == null || entity.trim().isEmpty()) {
      errors.add("Entity is required");
    } else if (!lexicon.getEntities().contains(entity.toLowerCase())) {
      errors.add("Unknown entity: " + entity + ". Valid: " + lexicon.getEntities());
    }
    
    return errors;
  }
  
  
  /**
   * Check if validation errors are simple structural issues that can be fixed quickly.
   */
  private boolean isSimpleValidationError(List<String> errors) {
    if (errors == null || errors.isEmpty()) return false;
    
    // Only try fast fix for simple structural errors
    for (String error : errors) {
      if (error.contains("Unknown entity") || error.contains("Unknown action")) {
        return true; // These are simple fixes
      }
      if (error.contains("PS is null") || error.contains("Operation is required") || 
          error.contains("Entity is required")) {
        return false; // These need full retry
      }
    }
    return false;
  }
  
  /**
   * Fast fix for JSON parsing errors using a focused prompt.
   * Attempts to extract and fix malformed JSON from the LLM response.
   * Uses generic "fix-json" phase name for all JSON parsing errors across all phases.
   */
  private NormalizationResult fixJsonParsingError(String rawResponse, String errorMsg, ConceptualLexicon lexicon, String phase)
      throws ExecutionException {
    // Create a simple fix prompt
    String fixPrompt = String.format(
        "The LLM response had a JSON parsing error. Fix the JSON by removing any wrappers or invalid syntax.\n\n" +
        "Error: %s\n\n" +
        "Raw response:\n%s\n\n" +
        "Extract and return ONLY the valid Prompt Schema JSON. Remove any wrappers like 'Optional[...]', '```json', etc. " +
        "Return pure JSON only, no explanation.",
        errorMsg.substring(0, Math.min(200, errorMsg.length())),
        rawResponse.substring(0, Math.min(2000, rawResponse.length()))
    );
    
    // Call LLM with a simple fix request
    LlmClient llmClient = oneMcp.llmClient();
    List<LlmClient.Message> messages = List.of(
        new LlmClient.Message(LlmClient.Role.SYSTEM, 
            "You are a JSON fixer. Extract and fix malformed JSON by removing wrappers and invalid syntax."),
        new LlmClient.Message(LlmClient.Role.USER, fixPrompt)
    );
    
    // Use generic fix-json phase name for all JSON parsing errors
    String fixPhase = "fix-json";
    
    long startTime = System.currentTimeMillis();
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logLlmInputMessages(messages);
      oneMcp.inferenceLogger().logLlmInferenceStart(fixPhase);
    }
    
    // Create telemetry sink to capture tokens
    final long[] promptTokens = {0};
    final long[] completionTokens = {0};
    
    LlmClient.TelemetrySink telemetrySink = new LlmClient.TelemetrySink() {
      @Override
      public void startChild(String name) {}
      
      @Override
      public void endCurrentOk(java.util.Map<String, Object> attrs) {}
      
      @Override
      public void endCurrentError(java.util.Map<String, Object> attrs) {}
      
      @Override
      public void addUsage(Long promptT, Long completionT, Long totalT) {
        if (promptT != null) promptTokens[0] = promptT;
        if (completionT != null) completionTokens[0] = completionT;
      }
      
      @Override
      public java.util.Map<String, Object> currentAttributes() {
        java.util.Map<String, Object> attrs = new HashMap<>();
        attrs.put("phase", fixPhase);
        return attrs;
      }
    };
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(telemetrySink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, 0.0f);
    } catch (Exception e) {
      throw new ExecutionException("LLM fix call failed: " + e.getMessage(), e);
    }
    
    long duration = System.currentTimeMillis() - startTime;
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logLlmInferenceComplete(fixPhase, duration, promptTokens[0], completionTokens[0], response);
    }
    
    if (response == null || response.trim().isEmpty()) {
      throw new ExecutionException("LLM returned empty fix response");
    }
    
    // Extract and parse fixed JSON
    String jsonContent = extractJsonFromResponse(response);
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      PromptSchema fixedPs = mapper.readValue(jsonContent, PromptSchema.class);
      return new NormalizationResult(fixedPs, null);
    } catch (Exception e) {
      throw new ExecutionException("Failed to parse fix response as JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Fast fix for validation errors using a focused prompt.
   * This is faster than a full retry because it only fixes the specific issues.
   * 
   * @param phase The phase name (e.g., "normalize", "lexifier-summary") for logging
   */
  private NormalizationResult fixValidationErrors(PromptSchema ps, List<String> errors, ConceptualLexicon lexicon, String phase)
      throws ExecutionException {
    if (ps == null) {
      throw new ExecutionException("Cannot fix null PS");
    }
    
    // Build a focused fix prompt
    String errorMsg = String.join("; ", errors);
    String psJson;
    try {
      psJson = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(ps);
    } catch (Exception e) {
      throw new ExecutionException("Failed to serialize PS for fix: " + e.getMessage(), e);
    }
    
    // Create a simple fix prompt
    String fixPrompt = String.format(
        "Fix the following validation errors in this Prompt Schema JSON:\n\n" +
        "Errors: %s\n\n" +
        "Valid vocabulary:\n" +
        "- Actions: %s\n" +
        "- Entities: %s\n\n" +
        "Current PS JSON:\n%s\n\n" +
        "Fix the PS by correcting the entity/action names to match the vocabulary exactly. " +
        "Return only the fixed JSON, no explanation.",
        errorMsg,
        lexicon.getActions(),
        lexicon.getEntities(),
        psJson
    );
    
    // Call LLM with a simple fix request
    LlmClient llmClient = oneMcp.llmClient();
    List<LlmClient.Message> messages = List.of(
        new LlmClient.Message(LlmClient.Role.SYSTEM, 
            "You are a JSON fixer. Fix validation errors in Prompt Schema JSON by correcting entity/action names to match the provided vocabulary."),
        new LlmClient.Message(LlmClient.Role.USER, fixPrompt)
    );
    
    // Use generic "fix-validation" phase name for all validation error fixes
    String fixPhase = "fix-validation";
    
    long startTime = System.currentTimeMillis();
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logLlmInputMessages(messages);
      oneMcp.inferenceLogger().logLlmInferenceStart(fixPhase);
    }
    
    // Create telemetry sink to capture tokens
    final long[] promptTokens = {0};
    final long[] completionTokens = {0};
    
    LlmClient.TelemetrySink telemetrySink = new LlmClient.TelemetrySink() {
      @Override
      public void startChild(String name) {}
      
      @Override
      public void endCurrentOk(java.util.Map<String, Object> attrs) {}
      
      @Override
      public void endCurrentError(java.util.Map<String, Object> attrs) {}
      
      @Override
      public void addUsage(Long promptT, Long completionT, Long totalT) {
        if (promptT != null) promptTokens[0] = promptT;
        if (completionT != null) completionTokens[0] = completionT;
      }
      
      @Override
      public java.util.Map<String, Object> currentAttributes() {
        // Return mutable map - AbstractLlmClient will call put() on it
        java.util.Map<String, Object> attrs = new HashMap<>();
        attrs.put("phase", fixPhase);
        return attrs;
      }
    };
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(telemetrySink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, 0.0f);
    } catch (Exception e) {
      throw new ExecutionException("LLM fix call failed: " + e.getMessage(), e);
    }
    
    long duration = System.currentTimeMillis() - startTime;
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logLlmInferenceComplete(fixPhase, duration, promptTokens[0], completionTokens[0], response);
    }
    
    if (response == null || response.trim().isEmpty()) {
      throw new ExecutionException("LLM returned empty fix response");
    }
    
    // Extract and parse fixed JSON
    String jsonContent = extractJsonFromResponse(response);
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      PromptSchema fixedPs = mapper.readValue(jsonContent, PromptSchema.class);
      return new NormalizationResult(fixedPs, null);
    } catch (Exception e) {
      throw new ExecutionException("Failed to parse fix response as JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Generate a cache key for failed normalization (based on prompt hash).
   */
  private String generateFailureCacheKey(String prompt) {
    String normalized = prompt.toLowerCase().trim().replaceAll("\\s+", " ");
    int hash = normalized.hashCode();
    return "failure:" + Integer.toHexString(hash);
  }

  /**
   * Result of a normalization attempt.
   */
  private static class NormalizationResult {
    final PromptSchema ps;
    final String note;

    NormalizationResult(PromptSchema ps, String note) {
      this.ps = ps;
      this.note = note;
    }
  }
}
