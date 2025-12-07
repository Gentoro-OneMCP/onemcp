package com.gentoro.onemcp.cache;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner service that converts Prompt Schema (PS) + conceptual schema + endpoint index → TypeScript execution plan.
 *
 * <p>Receives:
 * - PS.operation, PS.table (conceptual table)
 * - PS.fields, PS.where (expression tree), PS.group_by, PS.order_by, PS.limit, PS.offset
 * - PS.values (extracted literal values)
 * - get_endpoints(table) (endpoint index)
 * - Original prompt
 *
 * <p>Responsibilities:
 * - Walk WHERE expression tree recursively
 * - Identify relevant endpoints for the table
 * - Map WHERE → query params or client-side checks
 * - Map aggregates → client-side post-processing if needed
 * - Generate a complete TypeScript async function
 */
public class Planner {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(Planner.class);

  private final OneMcp oneMcp;
  private final Path cacheIndexDir;
  private final Path cacheDir;
  private final Path handbookPath;

  public Planner(OneMcp oneMcp, Path cacheIndexDir) {
    this.oneMcp = oneMcp;
    this.cacheIndexDir = cacheIndexDir;
    // Determine cache directory (parent of cacheIndexDir)
    this.cacheDir = cacheIndexDir.getParent().getParent();
    // Get handbook path for loading documentation
    this.handbookPath = oneMcp.handbook().location();
  }

  /**
   * Generate TypeScript execution plan from Prompt Schema (PS).
   *
   * @param ps the Prompt Schema
   * @param originalPrompt the original user prompt
   * @return TypeScript code as string
   * @throws ExecutionException if planning fails
   */
  public String plan(PromptSchema ps, String originalPrompt) 
      throws ExecutionException {
    return plan(ps, originalPrompt, null);
  }

  /**
   * Generate TypeScript execution plan from Prompt Schema (PS), with optional error feedback.
   *
   * @param ps the Prompt Schema
   * @param originalPrompt the original user prompt
   * @param previousError error message from a previous failed attempt (null if first attempt)
   * @return TypeScript code as string
   * @throws ExecutionException if planning fails
   */
  public String plan(PromptSchema ps, String originalPrompt, String previousError) 
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

    // Use original_prompt from PS if available, otherwise fall back to parameter
    String prompt = ps.getOriginalPrompt() != null ? ps.getOriginalPrompt() : originalPrompt;

    log.debug("Planning PS: operation={}, table={}", ps.getOperation(), ps.getTable());

    // Load endpoint index for this table (get_endpoints(table))
    TableEndpointIndex endpointIndex = loadEndpointIndex(ps.getTable());
    if (endpointIndex == null || endpointIndex.getEndpoints().isEmpty()) {
      throw new ExecutionException("No endpoints found for table: " + ps.getTable());
    }

    // Load API preamble (API description + docs) from cache
    String preamble = loadPreamble();
    String openApiDescription;
    String allDocsContent;
    
    if (preamble != null && !preamble.isEmpty()) {
      // Use cached API preamble
      log.debug("Using cached API preamble for planner");
      // Split preamble into API description and docs sections
      String[] parts = preamble.split("\n\n---\n\n# Documentation\n\n", 2);
      if (parts.length >= 1) {
        // Extract API description (remove "# API Description" header)
        String apiSection = parts[0].replaceFirst("^# API Description\\s*\n\\s*", "");
        openApiDescription = apiSection.trim();
      } else {
        openApiDescription = "";
      }
      if (parts.length >= 2) {
        allDocsContent = parts[1].trim();
      } else {
        allDocsContent = "";
      }
    } else {
      // Fallback: load separately (for backward compatibility or if cache doesn't exist)
      log.debug("API preamble not found in cache, loading separately");
      openApiDescription = loadOpenApiDescription();
      allDocsContent = loadAllDocumentation();
    }

    // Build consolidated input JSON for the planner prompt
    Map<String, Object> inputData = new HashMap<>();
    inputData.put("prompt", prompt);
    inputData.put("table", ps.getTable());
    
    // Build PS object with ONLY filter structure (cache key components)
    // EXCLUDED: fields, order_by, limit, offset - these are passed at runtime via options
    // This ensures plans are generic and can handle any output configuration
    Map<String, Object> psData = new HashMap<>();
    psData.put("operation", ps.getOperation());
    psData.put("where", ps.getWhere());
    psData.put("group_by", ps.getGroupBy() != null ? ps.getGroupBy() : java.util.Collections.emptyList());
    // Note: values are still included as they're needed for filter logic (but excluded from cache key)
    psData.put("values", ps.getValues() != null ? ps.getValues() : new HashMap<>());
    if (ps.getCurrentTime() != null) {
      psData.put("current_time", ps.getCurrentTime());
    }
    // fields, order_by, limit, offset are EXCLUDED - plan must handle these dynamically
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
    
    // Add API docs (openapi description + documentation)
    Map<String, Object> apiDocs = new HashMap<>();
    apiDocs.put("openapi", openApiDescription);
    apiDocs.put("docs", allDocsContent);
    inputData.put("api_docs", apiDocs);
    
    // Add previous error if this is a retry attempt
    if (previousError != null && !previousError.trim().isEmpty()) {
      inputData.put("previous_error", previousError);
      log.info("Including previous error in plan request for retry: {}", 
          previousError.length() > 200 ? previousError.substring(0, 200) + "..." : previousError);
    }
    
    // Serialize the consolidated input to JSON
    String inputJson;
    try {
      inputJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(inputData);
    } catch (Exception e) {
      log.error("Failed to serialize planner input JSON", e);
      throw new ExecutionException("Failed to serialize planner input JSON: " + e.getMessage(), e);
    }

    // Load and render PS planning prompt
    Map<String, Object> context = new HashMap<>();
    context.put("input_json", inputJson);
    
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("ps-plan");
    PromptTemplate.PromptSession session = template.newSession();
    
    // Use retry activation if we have a previous error, otherwise use standard activation
    if (previousError != null && !previousError.trim().isEmpty()) {
      context.put("previous_error", previousError);
      session.enable("ps-plan", context);  // Enable system prompt
      session.enable("ps-plan-retry", context);  // Enable retry user prompt with error
      log.debug("Using ps-plan-retry activation with previous error feedback");
    } else {
      session.enable("ps-plan", context);
    }

    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render PS planning prompt template: {}", e.getMessage(), e);
      throw new ExecutionException(
          "Failed to render PS planning prompt template: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from PS planning prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    log.debug("Calling LLM for PS planning");
    
    // Create telemetry sink to set phase name for logging
    final Map<String, Object> sinkAttributes = new HashMap<>();
    sinkAttributes.put("phase", "plan");
    sinkAttributes.put("cacheHit", false);
    
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
            // Token usage is tracked by LLM client automatically
          }
          
          @Override
          public java.util.Map<String, Object> currentAttributes() {
            return sinkAttributes;
          }
        };
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, null);
    } catch (Exception e) {
      log.error("LLM call failed for PS planning", e);
      throw new ExecutionException("LLM call failed for PS planning: " + e.getMessage(), e);
    }

    // Extract TypeScript code from response
    String typescript = extractTypeScriptFromResponse(response);
    if (typescript == null || typescript.trim().isEmpty()) {
      log.error("No TypeScript content found in LLM response");
      log.debug("LLM response was: {}", response);
      throw new ExecutionException("No TypeScript content found in LLM response");
    }

      log.info("Successfully generated TypeScript plan for PS: operation={}, table={}", 
          ps.getOperation(), ps.getTable());
    return typescript.trim();
  }

  /**
   * Generate DAG execution plan from Prompt Schema (PS).
   *
   * @param ps the Prompt Schema
   * @param originalPrompt the original user prompt
   * @return DAG JSON as string
   * @throws ExecutionException if planning fails
   */
  public String planDag(PromptSchema ps, String originalPrompt) 
      throws ExecutionException {
    return planDag(ps, originalPrompt, null);
  }

  /**
   * Generate DAG execution plan from Prompt Schema (PS), with optional error feedback.
   *
   * @param ps the Prompt Schema
   * @param originalPrompt the original user prompt
   * @param previousError error message from a previous failed attempt (null if first attempt)
   * @return DAG JSON as string
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

    // Use original_prompt from PS if available, otherwise fall back to parameter
    String prompt = ps.getOriginalPrompt() != null ? ps.getOriginalPrompt() : originalPrompt;

    log.debug("Planning DAG for PS: operation={}, table={}", ps.getOperation(), ps.getTable());

    // Load endpoint index for this table (get_endpoints(table))
    TableEndpointIndex endpointIndex = loadEndpointIndex(ps.getTable());
    if (endpointIndex == null || endpointIndex.getEndpoints().isEmpty()) {
      throw new ExecutionException("No endpoints found for table: " + ps.getTable());
    }

    // Load API preamble (API description + docs) from cache
    String preamble = loadPreamble();
    String openApiDescription;
    String allDocsContent;
    
    if (preamble != null && !preamble.isEmpty()) {
      // Use cached API preamble
      log.debug("Using cached API preamble for planner");
      // Split preamble into API description and docs sections
      String[] parts = preamble.split("\n\n---\n\n# Documentation\n\n", 2);
      if (parts.length >= 1) {
        // Extract API description (remove "# API Description" header)
        String apiSection = parts[0].replaceFirst("^# API Description\\s*\n\\s*", "");
        openApiDescription = apiSection.trim();
      } else {
        openApiDescription = "";
      }
      if (parts.length >= 2) {
        allDocsContent = parts[1].trim();
      } else {
        allDocsContent = "";
      }
    } else {
      // Fallback: load separately (for backward compatibility or if cache doesn't exist)
      log.debug("API preamble not found in cache, loading separately");
      openApiDescription = loadOpenApiDescription();
      allDocsContent = loadAllDocumentation();
    }

    // Build consolidated input JSON for the planner prompt
    Map<String, Object> inputData = new HashMap<>();
    inputData.put("prompt", prompt);
    inputData.put("table", ps.getTable());
    
    // Build PS object with ONLY filter structure (cache key components)
    // EXCLUDED: fields, order_by, limit, offset - these are passed at runtime via options
    // This ensures plans are generic and can handle any output configuration
    Map<String, Object> psData = new HashMap<>();
    psData.put("operation", ps.getOperation());
    psData.put("where", ps.getWhere());
    psData.put("group_by", ps.getGroupBy() != null ? ps.getGroupBy() : java.util.Collections.emptyList());
    // Note: values are still included as they're needed for filter logic (but excluded from cache key)
    psData.put("values", ps.getValues() != null ? ps.getValues() : new HashMap<>());
    if (ps.getCurrentTime() != null) {
      psData.put("current_time", ps.getCurrentTime());
    }
    // fields, order_by, limit, offset are EXCLUDED - plan must handle these dynamically
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
    
    // Add API docs (openapi description + documentation)
    Map<String, Object> apiDocs = new HashMap<>();
    apiDocs.put("openapi", openApiDescription);
    apiDocs.put("docs", allDocsContent);
    inputData.put("api_docs", apiDocs);
    
    // Add previous error if this is a retry attempt
    if (previousError != null && !previousError.trim().isEmpty()) {
      inputData.put("previous_error", previousError);
      log.info("Including previous error in DAG plan request for retry: {}", 
          previousError.length() > 200 ? previousError.substring(0, 200) + "..." : previousError);
    }
    
    // Serialize the consolidated input to JSON
    String inputJson;
    try {
      inputJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(inputData);
    } catch (Exception e) {
      log.error("Failed to serialize planner input JSON", e);
      throw new ExecutionException("Failed to serialize planner input JSON: " + e.getMessage(), e);
    }

    // Load and render DAG planning prompt
    Map<String, Object> context = new HashMap<>();
    context.put("input_json", inputJson);
    
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("ps-plan-dag");
    PromptTemplate.PromptSession session = template.newSession();
    
    // Use retry activation if we have a previous error, otherwise use standard activation
    if (previousError != null && !previousError.trim().isEmpty()) {
      context.put("previous_error", previousError);
      session.enable("ps-plan-dag", context);  // Enable system prompt
      // Note: ps-plan-dag-retry activation would need to be added to the prompt template
      log.debug("Using ps-plan-dag activation with previous error feedback");
    } else {
      session.enable("ps-plan-dag", context);
    }

    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render DAG planning prompt template: {}", e.getMessage(), e);
      throw new ExecutionException(
          "Failed to render DAG planning prompt template: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from DAG planning prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    log.debug("Calling LLM for DAG planning");
    
    // Create telemetry sink to set phase name for logging
    final Map<String, Object> sinkAttributes = new HashMap<>();
    sinkAttributes.put("phase", "plan-dag");
    sinkAttributes.put("cacheHit", false);
    
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
            // Token usage is tracked by LLM client automatically
          }
          
          @Override
          public java.util.Map<String, Object> currentAttributes() {
            return sinkAttributes;
          }
        };
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, null);
    } catch (Exception e) {
      log.error("LLM call failed for DAG planning", e);
      throw new ExecutionException("LLM call failed for DAG planning: " + e.getMessage(), e);
    }

    // Extract DAG JSON from response
    String dagJson = extractDagFromResponse(response);
    if (dagJson == null || dagJson.trim().isEmpty()) {
      log.error("No DAG JSON content found in LLM response");
      log.debug("LLM response was: {}", response);
      throw new ExecutionException("No DAG JSON content found in LLM response");
    }

    // Validate that it's valid JSON
    try {
      JacksonUtility.getJsonMapper().readTree(dagJson);
    } catch (Exception e) {
      log.error("Generated DAG is not valid JSON: {}", e.getMessage());
      log.debug("DAG JSON was: {}", dagJson);
      throw new ExecutionException("Generated DAG is not valid JSON: " + e.getMessage(), e);
    }

    log.info("Successfully generated DAG plan for PS: operation={}, table={}", 
        ps.getOperation(), ps.getTable());
    return dagJson.trim();
  }

  /**
   * Load endpoint index for a table from cache/endpoint_info/<table_name>.json.
   */
  private TableEndpointIndex loadEndpointIndex(String tableName) throws ExecutionException {
    Path indexFile = cacheIndexDir.resolve(tableName + ".json");
    
    if (!Files.exists(indexFile)) {
      log.warn("Endpoint index file not found: {}", indexFile);
      return null;
    }
    
    try {
      String content = Files.readString(indexFile);
      return JacksonUtility.getJsonMapper().readValue(content, TableEndpointIndex.class);
    } catch (IOException e) {
      log.error("Failed to load endpoint index: {}", indexFile, e);
      throw new ExecutionException("Failed to load endpoint index: " + e.getMessage(), e);
    }
  }

  /**
   * Load OpenAPI description from cache.
   */
  private String loadOpenApiDescription() {
    try {
      java.nio.file.Path openApiInfoFile = cacheDir.resolve("openapi_info.json");
      if (!Files.exists(openApiInfoFile)) {
        log.debug("OpenAPI info file not found: {}", openApiInfoFile);
        return "";
      }
      
      String content = Files.readString(openApiInfoFile);
      @SuppressWarnings("unchecked")
      Map<String, Object> info = JacksonUtility.getJsonMapper().readValue(content, Map.class);
      
      StringBuilder desc = new StringBuilder();
      if (info.get("title") != null) {
        desc.append("Title: ").append(info.get("title")).append("\n");
      }
      if (info.get("description") != null) {
        desc.append("Description: ").append(info.get("description")).append("\n");
      }
      if (info.get("version") != null) {
        desc.append("Version: ").append(info.get("version")).append("\n");
      }
      
      return desc.toString();
    } catch (Exception e) {
      log.warn("Failed to load OpenAPI description from cache: {}", e.getMessage());
      return "";
    }
  }

  /**
   * Load API preamble (API description + docs) from cache file.
   * The preamble contains the OpenAPI main description and all documentation from docs/.
   * This preamble is combined with table-specific endpoints to form the full api_context(table_name).
   * Returns null if file doesn't exist.
   */
  private String loadPreamble() {
    try {
      if (cacheDir == null) {
        return null;
      }
      Path preambleFile = cacheDir.resolve("api_preamble.md");
      if (Files.exists(preambleFile)) {
        String content = Files.readString(preambleFile);
        log.debug("Loaded API preamble from cache: {}", preambleFile);
        return content;
      }
    } catch (Exception e) {
      log.warn("Failed to load API preamble from cache: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Load all documentation files from the handbook's docs/ directory.
   * Returns concatenated content of all markdown files.
   */
  private String loadAllDocumentation() {
    StringBuilder allDocs = new StringBuilder();
    
    try {
      if (handbookPath == null) {
        log.debug("Handbook path not available, skipping documentation loading");
        return "";
      }
      
      Path docsPath = handbookPath.resolve("docs");
      if (Files.exists(docsPath) && Files.isDirectory(docsPath)) {
        List<Path> docFiles = Files.walk(docsPath)
            .filter(Files::isRegularFile)
            .filter(p -> {
              String name = p.getFileName().toString().toLowerCase();
              return name.endsWith(".md");
            })
            .sorted() // Sort for consistent ordering
            .toList();
        
        for (Path docFile : docFiles) {
          try {
            String content = Files.readString(docFile);
            String relativePath = handbookPath.relativize(docFile).toString();
            allDocs.append("## ").append(relativePath).append("\n\n");
            allDocs.append(content).append("\n\n");
          } catch (Exception e) {
            log.warn("Failed to read documentation file: {}", docFile, e);
          }
        }
        
        if (!docFiles.isEmpty()) {
          log.debug("Loaded {} documentation file(s) for planner from: {}", docFiles.size(), docsPath);
        }
      } else {
        log.debug("No docs directory found at: {}", docsPath);
      }
    } catch (Exception e) {
      log.warn("Failed to load documentation from handbook: {}", e.getMessage());
    }
    
    return allDocs.toString();
  }

  /**
   * Extract TypeScript code from LLM response, handling markdown code blocks.
   */
  private String extractTypeScriptFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String code = response.trim();

    // Remove markdown code block markers
    // Check for specific language tags first (longer matches first)
    if (code.startsWith("```typescript")) {
      code = code.substring(13).trim();
    } else if (code.startsWith("```javascript")) {
      code = code.substring(13).trim();
    } else if (code.startsWith("```js")) {
      code = code.substring(5).trim();
    } else if (code.startsWith("```ts")) {
      code = code.substring(5).trim();
    } else if (code.startsWith("```")) {
      code = code.substring(3).trim();
    }

    if (code.endsWith("```")) {
      code = code.substring(0, code.length() - 3).trim();
    }

    // Defensive: Remove "javascript" if it appears as the first line (leftover from old buggy extraction)
    // This handles cached code that was generated before the fix
    code = code.trim();
    if (code.startsWith("javascript\n") || code.startsWith("javascript\r\n")) {
      code = code.substring(10).trim(); // Remove "javascript\n" (10 chars) or "javascript\r\n" (12 chars)
    } else if (code.equals("javascript")) {
      code = ""; // Edge case: code is just "javascript"
    }

    return code.trim();
  }

  /**
   * Extract DAG JSON from LLM response, handling markdown code blocks.
   */
  private String extractDagFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String json = response.trim();

    // Remove markdown code block markers
    // Check for specific language tags first (longer matches first)
    if (json.startsWith("```json")) {
      json = json.substring(7).trim();
    } else if (json.startsWith("```")) {
      json = json.substring(3).trim();
    }

    if (json.endsWith("```")) {
      json = json.substring(0, json.length() - 3).trim();
    }

    return json.trim();
  }
}


