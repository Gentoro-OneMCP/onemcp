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
 * Planner service that converts S-SQL + conceptual schema + endpoint index → TypeScript execution plan 
 * (cache_spec v10.0).
 *
 * <p>Receives:
 * - PS.table (conceptual table)
 * - PS.ssql (S-SQL statement)
 * - get_endpoints(table) (endpoint index)
 * - Original prompt
 *
 * <p>Responsibilities:
 * - Parse S-SQL AST
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
    // Determine cache directory (parent of cacheIndexDir, which is typically cache/transducer/tables)
    this.cacheDir = cacheIndexDir.getParent().getParent();
    // Get handbook path for loading documentation
    this.handbookPath = oneMcp.handbook().location();
  }

  /**
   * Generate TypeScript execution plan from Prompt Schema (PS).
   *
   * @param ps the Prompt Schema containing { table, ssql, columns, cache_key }
   * @param originalPrompt the original user prompt
   * @return TypeScript code as string
   * @throws ExecutionException if planning fails
   */
  public String plan(PromptSchema ps, String originalPrompt) 
      throws ExecutionException {
    if (ps == null) {
      throw new IllegalArgumentException("Prompt Schema cannot be null");
    }
    if (ps.getSsql() == null || ps.getSsql().trim().isEmpty()) {
      throw new IllegalArgumentException("S-SQL cannot be null or empty");
    }
    if (ps.getTable() == null || ps.getTable().trim().isEmpty()) {
      throw new IllegalArgumentException("Table cannot be null or empty");
    }

    log.debug("Planning S-SQL: {}", ps.getSsql());

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

    // Prepare prompt context
    // Include current_time but exclude cache_key (not useful to planner)
    // Use canonical S-SQL (rendered from JSON IR if available)
    Map<String, Object> context = new HashMap<>();
    context.put("ssql", ps.getSsql()); // Canonical S-SQL (rendered from JSON IR)
    context.put("original_prompt", originalPrompt);
    context.put("table_name", ps.getTable());
    context.put("columns", ps.getColumns() != null ? ps.getColumns() : java.util.Collections.emptyList());
    context.put("values", ps.getValues() != null ? ps.getValues() : java.util.Collections.emptyMap());
    if (ps.getCurrentTime() != null) {
      context.put("current_time", ps.getCurrentTime());
    }
    // Include JSON IR if available (for future enhancements)
    if (ps.getJsonIr() != null) {
      try {
        String jsonIrStr = com.gentoro.onemcp.utility.JacksonUtility.getJsonMapper()
            .writeValueAsString(ps.getJsonIr());
        context.put("json_ir", jsonIrStr);
      } catch (Exception e) {
        log.debug("Failed to serialize JSON IR for planner context", e);
      }
    }
    
    // Combine all API context into a single JSON string
    try {
      // Serialize endpoint index to JSON first
      String endpointIndexJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(endpointIndex);
      
      // Parse it back to a Map so it's properly included in the final JSON
      @SuppressWarnings("unchecked")
      Map<String, Object> endpointIndexMap = JacksonUtility.getJsonMapper()
          .readValue(endpointIndexJson, Map.class);
      
      Map<String, Object> apiContext = new HashMap<>();
      apiContext.put("openapi_description", openApiDescription);
      apiContext.put("documentation", allDocsContent);
      apiContext.put("endpoint_index", endpointIndexMap);
      
      String apiContextJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(apiContext);
      context.put("api_context", apiContextJson);
    } catch (Exception e) {
      log.error("Failed to serialize API context", e);
      throw new ExecutionException("Failed to serialize API context: " + e.getMessage(), e);
    }

    // Load and render S-SQL planning prompt
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("ssql-plan");
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("ssql-plan", context);

    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render SQL planning prompt template: {}", e.getMessage(), e);
      throw new ExecutionException(
          "Failed to render SQL planning prompt template: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from SQL planning prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    log.debug("Calling LLM for SQL planning");
    
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
      log.error("LLM call failed for SQL planning", e);
      throw new ExecutionException("LLM call failed for SQL planning: " + e.getMessage(), e);
    }

    // Extract TypeScript code from response
    String typescript = extractTypeScriptFromResponse(response);
    if (typescript == null || typescript.trim().isEmpty()) {
      log.error("No TypeScript content found in LLM response");
      log.debug("LLM response was: {}", response);
      throw new ExecutionException("No TypeScript content found in LLM response");
    }

    log.info("Successfully generated TypeScript plan for S-SQL: {}", ps.getSsql());
    return typescript.trim();
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
    if (code.startsWith("```typescript")) {
      code = code.substring(13).trim();
    } else if (code.startsWith("```ts")) {
      code = code.substring(5).trim();
    } else if (code.startsWith("```")) {
      code = code.substring(3).trim();
    }

    if (code.endsWith("```")) {
      code = code.substring(0, code.length() - 3).trim();
    }

    return code.trim();
  }
}

