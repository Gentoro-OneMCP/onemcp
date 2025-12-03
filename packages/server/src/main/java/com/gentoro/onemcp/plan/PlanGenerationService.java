package com.gentoro.onemcp.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.responses.ApiResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generates parameterized execution plans from PromptSchema in the caching path.
 * 
 * <p>This is the plan generation service for the caching path. It generates
 * ExecutionPlan objects with placeholders like {{params.field}} that can be
 * cached and reused across different parameter values.
 */
public class PlanGenerationService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(PlanGenerationService.class);

  private final PlanContext context;

  public PlanGenerationService(PlanContext context) {
    this.context = context;
  }

  /**
   * Generate a parameterized execution plan from a PromptSchema.
   *
   * @param prompt the original natural-language prompt
   * @param schema the normalized PromptSchema
   * @return an ExecutionPlan with placeholders for parameter values
   * @throws ExecutionException if plan generation fails
   */
  public ExecutionPlan generatePlan(String prompt, PromptSchema schema) throws ExecutionException {
    return generatePlan(prompt, schema, null);
  }

  /**
   * Generate a parameterized execution plan with optional execution error feedback.
   *
   * @param prompt the original natural-language prompt
   * @param schema the normalized PromptSchema
   * @param executionError optional error message from a previous execution attempt
   * @return an ExecutionPlan with placeholders for parameter values
   * @throws ExecutionException if plan generation fails
   */
  public ExecutionPlan generatePlan(String prompt, PromptSchema schema, String executionError) 
      throws ExecutionException {
    log.info("Generating parameterized plan for PSK: {}", schema.getCacheKey());
    if (executionError != null) {
      log.info("Retrying with execution error feedback: {}", executionError);
    }
    log.debug("Original prompt: {}", prompt);
    log.debug("Schema: {}", schema);

    // Get available operations from handbook
    List<String> availableOperations = getAvailableOperations();
    log.debug("Available operations: {}", availableOperations);

    // Build operation documentation for the prompt
    String operationDocs = buildOperationDocumentation();

    // Set original prompt on schema for serialization
    schema.setOriginalPrompt(prompt);
    
    // Prepare prompt context
    Map<String, Object> promptContext = new HashMap<>();
    promptContext.put("schema_json", serializeSchema(schema));
    promptContext.put("available_operations", availableOperations);
    promptContext.put("operation_docs", operationDocs);

    // Load and render the plan generation prompt
    com.gentoro.onemcp.prompt.PromptTemplate template;
    try {
      template = context.prompts().get("/plan-generation-schema-aware");
    } catch (Exception e) {
      // Try without leading slash
      try {
        template = context.prompts().get("plan-generation-schema-aware");
      } catch (Exception e2) {
        throw new ExecutionException(
            "Failed to load prompt template 'plan-generation-schema-aware'. " +
            "Please ensure the file 'prompts/plan-generation-schema-aware.yaml' exists.", e);
      }
    }

    com.gentoro.onemcp.prompt.PromptTemplate.PromptSession session = template.newSession();
    session.enable("plan-generation-schema-aware", promptContext);

    List<com.gentoro.onemcp.model.LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render plan generation template", e);
      throw new ExecutionException("Failed to render plan generation prompt: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from plan generation prompt");
    }

    // Call LLM with retry
    com.gentoro.onemcp.model.LlmClient llmClient = context.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    int maxAttempts = 3;
    List<String> previousErrors = new ArrayList<>();
    
    if (executionError != null && !executionError.trim().isEmpty()) {
      previousErrors.add("Execution failed: " + executionError);
    }

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      List<com.gentoro.onemcp.model.LlmClient.Message> messagesToSend = new ArrayList<>(messages);

      // Add feedback from previous attempt if retrying
      if (attempt > 1 && !previousErrors.isEmpty()) {
        StringBuilder feedback = new StringBuilder();
        feedback.append("Your previous response was REJECTED due to errors:\n\n");
        for (String error : previousErrors) {
          feedback.append("❌ ").append(error).append("\n");
        }
        feedback.append("\nPlease fix these issues and try again.\n");
        messagesToSend.add(new com.gentoro.onemcp.model.LlmClient.Message(
            com.gentoro.onemcp.model.LlmClient.Role.USER, feedback.toString()));
        log.warn("Retrying plan generation (attempt {}/{})", attempt, maxAttempts);
      }

      try {
        // Set phase on telemetry sink for proper logging
        com.gentoro.onemcp.orchestrator.OrchestratorTelemetrySink sink = 
            new com.gentoro.onemcp.orchestrator.OrchestratorTelemetrySink(
                context.tracer(), 
                new com.gentoro.onemcp.orchestrator.OrchestratorContext(
                    context.oneMcp(), context.memory()));
        sink.setPhase("plan");
        // Plan generation only happens on cache miss
        sink.setCacheHit(false);
        
        // Get temperature from template if specified
        Float temperature = template.temperature().orElse(null);
        
        // Call LLM with telemetry using chat() to preserve message structure
        String response;
        try (com.gentoro.onemcp.model.LlmClient.TelemetryScope ignored = 
            llmClient.withTelemetry(sink)) {
          // Use chat() instead of generate() to preserve message structure and proper logging
          // chat() will automatically log input messages
          response = llmClient.chat(messagesToSend, List.of(), false, null, temperature);
        }

        if (response == null || response.trim().isEmpty()) {
          previousErrors.clear();
          previousErrors.add("LLM returned empty response");
          continue;
        }

        // Extract JSON from response
        String jsonStr = extractJsonFromResponse(response);
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
          // Log the failed plan (no JSON)
          try {
            context.oneMcp().inferenceLogger().logExecutionPlan(
                response, null, "No JSON found in LLM response");
          } catch (Exception logErr) {
            log.warn("Failed to log no-JSON error", logErr);
          }
          
          previousErrors.clear();
          previousErrors.add("No JSON found in LLM response");
          continue;
        }

        // Parse and validate the plan
        JsonNode planNode = JacksonUtility.getJsonMapper().readTree(jsonStr);
        
        // Basic validation
        List<String> validationErrors = validatePlan(planNode, availableOperations);
        if (!validationErrors.isEmpty()) {
          // Log the failed plan with validation errors
          String errorMsg = String.join("; ", validationErrors);
          try {
            context.oneMcp().inferenceLogger().logExecutionPlan(
                jsonStr, null, "Validation failed: " + errorMsg);
          } catch (Exception logErr) {
            log.warn("Failed to log validation error for plan", logErr);
          }
          
          previousErrors.clear();
          previousErrors.addAll(validationErrors);
          continue;
        }

        // Success! Log the valid plan
        ExecutionPlan plan = ExecutionPlan.fromNode(planNode);
        log.info("Successfully generated plan (attempt {})", attempt);
        log.debug("Generated plan: {}", plan.toPrettyJson());
        return plan;

      } catch (Exception e) {
        log.error("Plan generation failed (attempt {}): {}", attempt, e.getMessage());
        previousErrors.clear();
        previousErrors.add("Exception: " + e.getMessage());
      }
    }

    throw new ExecutionException(
        "Failed to generate execution plan after " + maxAttempts + " attempts. " +
        "Last errors: " + String.join(", ", previousErrors));
  }

  /**
   * Get list of available operation names from the handbook.
   */
  private List<String> getAvailableOperations() {
    if (context.handbook() == null || context.handbook().apis() == null) {
      return Collections.emptyList();
    }
    return context.handbook().apis().values().stream()
        .flatMap(a -> a.getService().getOpenApi().getPaths().values().stream())
        .flatMap(p -> p.readOperations().stream())
        .map(Operation::getOperationId)
        .filter(opId -> opId != null)
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Build operation documentation as markdown string, then JSON-stringify it.
   * Includes full operation details: description, input schema, output schema, and parameters.
   */
  private String buildOperationDocumentation() {
    if (context.handbook() == null || context.handbook().apis() == null) {
      return "{\"documentation\":\"No operations available.\"}";
    }

    StringBuilder sb = new StringBuilder();
    for (com.gentoro.onemcp.handbook.model.agent.Api api : context.handbook().apis().values()) {
      if (api.getService() == null || api.getService().getOpenApi() == null) {
        continue;
      }
      
      sb.append("### Service: ").append(api.getSlug()).append("\n\n");
      
      var openApi = api.getService().getOpenApi();
      if (openApi.getPaths() != null) {
        for (var pathEntry : openApi.getPaths().entrySet()) {
          var pathItem = pathEntry.getValue();
          for (var method : pathItem.readOperationsMap().entrySet()) {
            var op = method.getValue();
            if (op.getOperationId() == null) {
              continue;
            }
            
            sb.append("#### `").append(op.getOperationId()).append("`\n\n");
            
            // Description/Summary
            if (op.getDescription() != null && !op.getDescription().isEmpty()) {
              sb.append(op.getDescription()).append("\n\n");
            } else if (op.getSummary() != null && !op.getSummary().isEmpty()) {
              sb.append(op.getSummary()).append("\n\n");
            }
            
            // Input Schema (from requestBody)
            String inputSchema = extractInputSchema(op);
            if (inputSchema != null && !inputSchema.trim().isEmpty()) {
              sb.append("**Input Schema:**\n");
              sb.append("```json\n");
              sb.append(inputSchema);
              sb.append("\n```\n\n");
            }
            
            // Parameters (query/path/header parameters)
            if (op.getParameters() != null && !op.getParameters().isEmpty()) {
              sb.append("**Parameters:**\n");
              for (var param : op.getParameters()) {
                sb.append("- `").append(param.getName()).append("`");
                if (param.getRequired() != null && param.getRequired()) {
                  sb.append(" (required)");
                }
                if (param.getDescription() != null && !param.getDescription().isEmpty()) {
                  sb.append(": ").append(param.getDescription());
                }
                sb.append("\n");
              }
              sb.append("\n");
            }
            
            // Output Schema (from responses)
            String outputSchema = extractOutputSchema(op);
            if (outputSchema != null && !outputSchema.trim().isEmpty()) {
              sb.append("**Output Schema:**\n");
              sb.append("```json\n");
              sb.append(outputSchema);
              sb.append("\n```\n\n");
            }
            
            sb.append("\n");
          }
        }
      }
    }
    
    // JSON-stringify the markdown string
    String markdown = sb.length() > 0 ? sb.toString() : "No operations available.";
    try {
      Map<String, Object> result = new HashMap<>();
      result.put("documentation", markdown);
      return JacksonUtility.getJsonMapper().writeValueAsString(result);
    } catch (Exception e) {
      log.error("Failed to JSON-stringify operation documentation", e);
      return "{\"documentation\":\"" + markdown.replace("\"", "\\\"").replace("\n", "\\n") + "\"}";
    }
  }

  /**
   * Extract input schema from operation requestBody.
   */
  private String extractInputSchema(io.swagger.v3.oas.models.Operation op) {
    if (op.getRequestBody() == null || op.getRequestBody().getContent() == null) {
      return null;
    }
    
    var content = op.getRequestBody().getContent();
    var jsonMediaType = content.get("application/json");
    if (jsonMediaType == null && !content.isEmpty()) {
      jsonMediaType = content.values().iterator().next();
    }
    
    if (jsonMediaType != null && jsonMediaType.getSchema() != null) {
      return serializeSchema(jsonMediaType.getSchema());
    }
    
    return null;
  }

  /**
   * Extract output schema from operation responses (first successful response).
   */
  private String extractOutputSchema(io.swagger.v3.oas.models.Operation op) {
    if (op.getResponses() == null || op.getResponses().isEmpty()) {
      return null;
    }
    
    // Look for first 2xx response
    io.swagger.v3.oas.models.responses.ApiResponse response = null;
    for (var entry : op.getResponses().entrySet()) {
      String statusCode = entry.getKey();
      if (statusCode.startsWith("2") || "default".equals(statusCode)) {
        response = entry.getValue();
        break;
      }
    }
    
    if (response == null || response.getContent() == null) {
      return null;
    }
    
    var content = response.getContent();
    var jsonMediaType = content.get("application/json");
    if (jsonMediaType == null && !content.isEmpty()) {
      jsonMediaType = content.values().iterator().next();
    }
    
    if (jsonMediaType != null && jsonMediaType.getSchema() != null) {
      return serializeSchema(jsonMediaType.getSchema());
    }
    
    return null;
  }

  /**
   * Serialize an OpenAPI Schema to JSON string.
   * Handles $ref references and converts to a simplified JSON Schema representation.
   */
  private String serializeSchema(io.swagger.v3.oas.models.media.Schema<?> schema) {
    if (schema == null) {
      return null;
    }
    
    try {
      Map<String, Object> schemaMap = new HashMap<>();
      
      // Handle $ref
      if (schema.get$ref() != null) {
        // Try to resolve if it's a component schema
        String refPath = schema.get$ref();
        if (refPath.startsWith("#/components/schemas/")) {
          String schemaName = refPath.substring("#/components/schemas/".length());
          // Try to get from handbook's OpenAPI
          for (var api : context.handbook().apis().values()) {
            if (api.getService() != null && api.getService().getOpenApi() != null) {
              var components = api.getService().getOpenApi().getComponents();
              if (components != null && components.getSchemas() != null) {
                var resolvedSchema = components.getSchemas().get(schemaName);
                if (resolvedSchema != null) {
                  // Use resolved schema directly (don't include $ref)
                  schemaMap = serializeSchemaToMap(resolvedSchema);
                  break;
                }
              }
            }
          }
          // If resolution failed, keep $ref as fallback
          if (schemaMap.isEmpty()) {
            schemaMap.put("$ref", refPath);
          }
        } else {
          schemaMap.put("$ref", refPath);
        }
      } else {
        schemaMap = serializeSchemaToMap(schema);
      }
      
      return JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(schemaMap);
    } catch (Exception e) {
      log.debug("Failed to serialize schema", e);
      return null;
    }
  }

  /**
   * Convert an OpenAPI Schema to a Map representation (simplified JSON Schema).
   * Handles $ref references recursively.
   */
  private Map<String, Object> serializeSchemaToMap(io.swagger.v3.oas.models.media.Schema<?> schema) {
    if (schema == null) {
      return new HashMap<>();
    }
    
    // Handle $ref first
    if (schema.get$ref() != null) {
      String refPath = schema.get$ref();
      if (refPath.startsWith("#/components/schemas/")) {
        String schemaName = refPath.substring("#/components/schemas/".length());
        // Try to resolve from handbook's OpenAPI
        for (var api : context.handbook().apis().values()) {
          if (api.getService() != null && api.getService().getOpenApi() != null) {
            var components = api.getService().getOpenApi().getComponents();
            if (components != null && components.getSchemas() != null) {
              var resolvedSchema = components.getSchemas().get(schemaName);
              if (resolvedSchema != null) {
                // Recursively serialize the resolved schema
                return serializeSchemaToMap(resolvedSchema);
              }
            }
          }
        }
      }
      // If resolution failed, return $ref
      Map<String, Object> refMap = new HashMap<>();
      refMap.put("$ref", refPath);
      return refMap;
    }
    
    Map<String, Object> schemaMap = new HashMap<>();
    
    if (schema.getType() != null) {
      schemaMap.put("type", schema.getType());
    }
    if (schema.getProperties() != null) {
      Map<String, Object> propertiesMap = new HashMap<>();
      for (var propEntry : schema.getProperties().entrySet()) {
        Map<String, Object> propMap = serializeSchemaToMap(propEntry.getValue());
        propertiesMap.put(propEntry.getKey(), propMap);
      }
      schemaMap.put("properties", propertiesMap);
    }
    if (schema.getRequired() != null && !schema.getRequired().isEmpty()) {
      schemaMap.put("required", new ArrayList<>(schema.getRequired()));
    }
    if (schema.getDescription() != null) {
      schemaMap.put("description", schema.getDescription());
    }
    if (schema.getItems() != null) {
      schemaMap.put("items", serializeSchemaToMap(schema.getItems()));
    }
    if (schema.getEnum() != null && !schema.getEnum().isEmpty()) {
      schemaMap.put("enum", new ArrayList<>(schema.getEnum()));
    }
    if (schema.getFormat() != null) {
      schemaMap.put("format", schema.getFormat());
    }
    
    return schemaMap;
  }

  /**
   * Serialize PromptSchema to JSON string for inclusion in the prompt.
   */
  private String serializeSchema(PromptSchema schema) {
    try {
      return JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(schema);
    } catch (Exception e) {
      log.error("Failed to serialize schema", e);
      return "{}";
    }
  }

  /**
   * Extract JSON content from LLM response.
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

    // Find JSON object boundaries
    int firstBrace = jsonStr.indexOf('{');
    int lastBrace = jsonStr.lastIndexOf('}');

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      jsonStr = jsonStr.substring(firstBrace, lastBrace + 1);
    }

    return jsonStr.trim();
  }

  /**
   * Validate the generated plan.
   */
  private List<String> validatePlan(JsonNode plan, List<String> availableOperations) {
    List<String> errors = new ArrayList<>();

    // Must have start_node
    if (!plan.has("start_node")) {
      errors.add("Plan must have a 'start_node'");
    }

    // Check all operation nodes use valid operations
    plan.fields().forEachRemaining(entry -> {
      String nodeId = entry.getKey();
      JsonNode nodeDef = entry.getValue();
      
      if (nodeDef.has("operation")) {
        String opName = nodeDef.get("operation").asText();
        if (!availableOperations.contains(opName)) {
          errors.add("Node '" + nodeId + "' uses unknown operation: " + opName + 
              ". Available: " + availableOperations);
        }
      }
    });

    // Must have at least one terminal node (completed: true)
    boolean hasTerminal = false;
    var fields = plan.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      JsonNode nodeDef = entry.getValue();
      if (nodeDef.has("completed") && nodeDef.get("completed").asBoolean()) {
        hasTerminal = true;
        break;
      }
    }
    if (!hasTerminal) {
      errors.add("Plan must have at least one terminal node with 'completed: true'");
    }

    return errors;
  }
}

