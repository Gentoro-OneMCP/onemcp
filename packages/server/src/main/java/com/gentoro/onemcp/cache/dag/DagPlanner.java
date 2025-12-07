package com.gentoro.onemcp.cache.dag;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.TableEndpointIndex;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.model.LlmClient;
import java.util.ArrayList;
import java.util.List;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Planner that generates DAG IR from Prompt Schema.
 * 
 * <p>This is the bridge from PS + API metadata → DAG IR.
 * Uses LLM to generate DAG JSON following the node vocabulary.
 */
public class DagPlanner {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(DagPlanner.class);

  private final OneMcp oneMcp;
  private final Path cacheIndexDir;
  private final Path handbookPath;

  public DagPlanner(OneMcp oneMcp, Path cacheIndexDir) {
    this.oneMcp = oneMcp;
    this.cacheIndexDir = cacheIndexDir;
    this.handbookPath = oneMcp.handbook().location();
  }

  /**
   * Generate DAG IR execution plan from Prompt Schema.
   * 
   * @param ps the Prompt Schema
   * @param originalPrompt the original user prompt
   * @return DAG plan JSON string
   * @throws ExecutionException if planning fails
   */
  public String planDag(PromptSchema ps, String originalPrompt) throws ExecutionException {
    return planDag(ps, originalPrompt, null);
  }

  /**
   * Generate DAG IR execution plan from Prompt Schema, with optional error feedback.
   * 
   * @param ps the Prompt Schema
   * @param originalPrompt the original user prompt
   * @param previousError error message from a previous failed attempt (null if first attempt)
   * @return DAG plan JSON string
   * @throws ExecutionException if planning fails
   */
  public String planDag(PromptSchema ps, String originalPrompt, String previousError) 
      throws ExecutionException {
    if (ps == null) {
      throw new IllegalArgumentException("Prompt Schema cannot be null");
    }
    if (ps.getOperation() == null || ps.getOperation().trim().isEmpty()) {
      throw new IllegalArgumentException("Operation cannot be null or empty");
    }
    if (ps.getTable() == null || ps.getTable().trim().isEmpty()) {
      throw new IllegalArgumentException("Table cannot be null or empty");
    }

    String prompt = ps.getOriginalPrompt() != null ? ps.getOriginalPrompt() : originalPrompt;

    log.debug("Planning DAG IR for PS: operation={}, table={}", ps.getOperation(), ps.getTable());

    // Load endpoint index
    TableEndpointIndex endpointIndex = loadEndpointIndex(ps.getTable());
    if (endpointIndex == null || endpointIndex.getEndpoints().isEmpty()) {
      throw new ExecutionException("No endpoints found for table: " + ps.getTable());
    }

    // Load API documentation
    String preamble = loadPreamble();
    String openApiDescription;
    String allDocsContent;

    if (preamble != null && !preamble.isEmpty()) {
      String[] parts = preamble.split("\n\n---\n\n# Documentation\n\n", 2);
      openApiDescription = parts.length >= 1 
          ? parts[0].replaceFirst("^# API Description\\s*\n\\s*", "").trim() 
          : "";
      allDocsContent = parts.length >= 2 ? parts[1].trim() : "";
    } else {
      openApiDescription = loadOpenApiDescription();
      allDocsContent = loadAllDocumentation();
    }

    // Build input JSON for planner prompt
    Map<String, Object> inputData = new HashMap<>();
    inputData.put("prompt", prompt);
    inputData.put("table", ps.getTable());

    // Build PS object (conceptual structure)
    Map<String, Object> psData = new HashMap<>();
    psData.put("action", ps.getOperation()); // PS uses "operation", spec uses "action"
    psData.put("entity", ps.getTable()); // PS uses "table", spec uses "entity"
    
    // Convert PS filter structure to spec format
    if (ps.getWhere() != null) {
      psData.put("filter", convertWhereToFilter(ps.getWhere()));
    }
    
    // Params from PS.values
    if (ps.getValues() != null && !ps.getValues().isEmpty()) {
      psData.put("params", ps.getValues());
    }
    
    // Shape (group_by, aggregates, order_by, limit, offset)
    Map<String, Object> shape = new HashMap<>();
    
    // Convert group_by
    if (ps.getGroupBy() != null && !ps.getGroupBy().isEmpty()) {
      shape.put("group_by", ps.getGroupBy());
    } else {
      shape.put("group_by", new ArrayList<>());
    }
    
    // Convert fields (with agg/column) to aggregates (with field/function)
    List<Map<String, Object>> aggregates = new ArrayList<>();
    if (ps.getFields() != null) {
      for (Object fieldObj : ps.getFields()) {
        if (fieldObj instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> field = (Map<String, Object>) fieldObj;
          Map<String, Object> aggregate = new HashMap<>();
          
          // Convert "column" to "field"
          if (field.containsKey("column")) {
            aggregate.put("field", field.get("column"));
          }
          
          // Convert "agg" to "function"
          if (field.containsKey("agg")) {
            aggregate.put("function", field.get("agg"));
          }
          
          if (!aggregate.isEmpty()) {
            aggregates.add(aggregate);
          }
        }
      }
    }
    shape.put("aggregates", aggregates);
    
    // Convert order_by
    if (ps.getOrderBy() != null && !ps.getOrderBy().isEmpty()) {
      shape.put("order_by", ps.getOrderBy());
    } else {
      shape.put("order_by", new ArrayList<>());
    }
    
    // Convert limit (integer to string per spec, or null)
    if (ps.getLimit() != null) {
      shape.put("limit", ps.getLimit().toString());
    } else {
      shape.put("limit", null);
    }
    
    // Convert offset (integer to string per spec, or null)
    if (ps.getOffset() != null) {
      shape.put("offset", ps.getOffset().toString());
    } else {
      shape.put("offset", null);
    }
    
    psData.put("shape", shape);
    
    if (ps.getCurrentTime() != null) {
      psData.put("current_time", ps.getCurrentTime());
    }
    
    inputData.put("ps", psData);

    // Add endpoint index
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> endpointIndexMap = JacksonUtility.getJsonMapper()
          .readValue(JacksonUtility.getJsonMapper().writeValueAsString(endpointIndex), Map.class);
      inputData.put("endpoints", endpointIndexMap);
    } catch (Exception e) {
      log.error("Failed to serialize endpoint index", e);
      throw new ExecutionException("Failed to serialize endpoint index: " + e.getMessage(), e);
    }

    // Add API docs
    Map<String, Object> apiDocs = new HashMap<>();
    apiDocs.put("openapi", openApiDescription);
    apiDocs.put("docs", allDocsContent);
    inputData.put("api_docs", apiDocs);

    // Add previous error if retry
    if (previousError != null && !previousError.trim().isEmpty()) {
      inputData.put("previous_error", previousError);
      log.info("Including previous error in DAG plan request for retry");
    }

    // Serialize input to JSON
    String inputJson;
    try {
      inputJson = JacksonUtility.getJsonMapper().writeValueAsString(inputData);
    } catch (Exception e) {
      log.error("Failed to serialize planner input", e);
      throw new ExecutionException("Failed to serialize planner input: " + e.getMessage(), e);
    }

    // Load planner prompt template
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("ps-plan-dag");
    if (template == null) {
      throw new ExecutionException("Prompt template 'ps-plan-dag' not found");
    }

    // Generate DAG IR using LLM (same pattern as Planner)
    Map<String, Object> context = new HashMap<>();
    context.put("input_json", inputJson);
    
    PromptTemplate.PromptSession session = template.newSession();
    
    // Use retry activation if we have a previous error
    if (previousError != null && !previousError.trim().isEmpty()) {
      context.put("previous_error", previousError);
      session.enable("ps-plan-dag", context);
      session.enable("ps-plan-dag-retry", context);
    } else {
      session.enable("ps-plan-dag", context);
    }
    
    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render DAG planning prompt template: {}", e.getMessage(), e);
      throw new ExecutionException("Failed to render DAG planning prompt template: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from DAG planning prompt");
    }
    
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }
    
    // Set phase name for logging
    String phaseName = "plan-dag";
    final Map<String, Object> telemetryAttrs = new HashMap<>();
    telemetryAttrs.put("phase", phaseName);
    
    // Create telemetry sink to capture tokens and set phase
    LlmClient.TelemetrySink telemetrySink = new LlmClient.TelemetrySink() {
      @Override
      public void startChild(String name) {
        // Log inference start when LLM client calls startChild (e.g., "llm.anthropic")
        if (name != null && name.startsWith("llm.") && oneMcp.inferenceLogger() != null) {
          oneMcp.inferenceLogger().logLlmInferenceStart(phaseName);
        }
      }
      
      @Override
      public void endCurrentOk(java.util.Map<String, Object> attrs) {
        // No-op
      }
      
      @Override
      public void endCurrentError(java.util.Map<String, Object> attrs) {
        // No-op
      }
      
      @Override
      public void addUsage(Long promptT, Long completionT, Long totalT) {
        // Token usage is already logged by the LLM client
      }
      
      @Override
      public java.util.Map<String, Object> currentAttributes() {
        // Return the same mutable map so phase persists
        return telemetryAttrs;
      }
    };
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(telemetrySink)) {
      // Get temperature from template
      Float temperature = template.temperature().orElse(0.0f);
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, temperature);
    } catch (Exception e) {
      log.error("LLM call failed during DAG planning", e);
      throw new ExecutionException("LLM call failed: " + e.getMessage(), e);
    }

    // Extract DAG JSON from response
    String dagJson = extractDagFromResponse(response);
    
    // Validate DAG structure
    try {
      DagPlan.fromJsonString(dagJson);
    } catch (Exception e) {
      log.error("Generated DAG is invalid: {}", e.getMessage());
      throw new ExecutionException("Generated DAG is invalid: " + e.getMessage(), e);
    }

    log.info("Generated DAG IR with {} nodes", DagPlan.fromJsonString(dagJson).getNodes().size());
    return dagJson;
  }

  private Object convertWhereToFilter(Object where) {
    // Convert PS where structure to spec filter format
    // This is a simplified conversion - may need more sophisticated handling
    // For now, pass through and let planner prompt handle it
    return where;
  }

  private String extractDagFromResponse(String response) {
    // Extract JSON from response - reject TypeScript/JavaScript
    String jsonContent = response.trim();
    
    // REJECT TypeScript/JavaScript code
    if (jsonContent.contains("export function") || 
        jsonContent.contains("async function") ||
        jsonContent.contains("export async function") ||
        jsonContent.contains("function run(")) {
      throw new ExecutionException(
          "LLM generated TypeScript/JavaScript code instead of DAG JSON. " +
          "Response must be JSON only, starting with { and containing 'nodes' array.");
    }
    
    // Remove markdown code block markers
    if (jsonContent.startsWith("```json")) {
      jsonContent = jsonContent.substring(7);
    } else if (jsonContent.startsWith("```")) {
      jsonContent = jsonContent.substring(3);
    }
    
    if (jsonContent.endsWith("```")) {
      jsonContent = jsonContent.substring(0, jsonContent.length() - 3);
    }
    
    jsonContent = jsonContent.trim();
    
    // Must start with { and contain "nodes"
    if (!jsonContent.startsWith("{")) {
      throw new ExecutionException(
          "LLM response is not valid JSON. Must start with {. " +
          "Got: " + jsonContent.substring(0, Math.min(200, jsonContent.length())));
    }
    
    // Reject old DAG format (value_conversions, start_node) - require format with nodes array
    if (jsonContent.contains("\"value_conversions\"") || jsonContent.contains("\"start_node\"")) {
      throw new ExecutionException(
          "LLM generated OLD DAG format (with value_conversions/start_node). " +
          "Must generate format with 'nodes' array and 'entryPoint'. " +
          "Response: " + jsonContent.substring(0, Math.min(500, jsonContent.length())));
    }
    
    // Must contain "nodes" array
    if (!jsonContent.contains("\"nodes\"")) {
      throw new ExecutionException(
          "LLM response is not valid DAG JSON. Must contain 'nodes' array. " +
          "Got: " + jsonContent.substring(0, Math.min(500, jsonContent.length())));
    }
    
    // Validate JSON
    try {
      JacksonUtility.getJsonMapper().readTree(jsonContent);
    } catch (Exception e) {
      throw new ExecutionException("Invalid JSON in planner response: " + e.getMessage(), e);
    }
    
    return jsonContent;
  }

  private TableEndpointIndex loadEndpointIndex(String table) {
    try {
      // cacheIndexDir is already the endpoint_info directory (passed from OrchestratorService)
      Path indexFile = cacheIndexDir.resolve(table + ".json");
      if (!java.nio.file.Files.exists(indexFile)) {
        log.warn("Endpoint index not found for table: {} at path: {}", table, indexFile);
        return null;
      }
      String content = java.nio.file.Files.readString(indexFile);
      return JacksonUtility.getJsonMapper().readValue(content, TableEndpointIndex.class);
    } catch (Exception e) {
      log.error("Failed to load endpoint index for table: {}", table, e);
      return null;
    }
  }

  private String loadPreamble() {
    try {
      Path preambleFile = cacheIndexDir.resolve("api_preamble.txt");
      if (java.nio.file.Files.exists(preambleFile)) {
        return java.nio.file.Files.readString(preambleFile);
      }
    } catch (Exception e) {
      log.debug("Failed to load API preamble", e);
    }
    return null;
  }

  private String loadOpenApiDescription() {
    try {
      Path openApiFile = handbookPath.resolve("apis").resolve("sales-analytics-api.yaml");
      if (java.nio.file.Files.exists(openApiFile)) {
        return java.nio.file.Files.readString(openApiFile);
      }
    } catch (Exception e) {
      log.debug("Failed to load OpenAPI description", e);
    }
    return "";
  }

  private String loadAllDocumentation() {
    // Load all documentation files from handbook
    try {
      Path docsDir = handbookPath.resolve("docs");
      if (java.nio.file.Files.exists(docsDir)) {
        StringBuilder docs = new StringBuilder();
        java.nio.file.Files.walk(docsDir)
            .filter(p -> p.toString().endsWith(".md") || p.toString().endsWith(".txt"))
            .forEach(p -> {
              try {
                docs.append(java.nio.file.Files.readString(p)).append("\n\n");
              } catch (Exception e) {
                log.debug("Failed to read doc file: {}", p, e);
              }
            });
        return docs.toString();
      }
    } catch (Exception e) {
      log.debug("Failed to load documentation", e);
    }
    return "";
  }
}

