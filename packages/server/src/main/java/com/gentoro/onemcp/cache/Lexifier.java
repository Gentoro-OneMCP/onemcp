package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lexifier - Extracts a minimal conceptual lexicon from API specification.
 * 
 * <p>The Lexifier reads the OpenAPI spec and produces a flat vocabulary:
 * - actions: canonical operations (search, update, create, delete)
 * - entities: semantic entities from the API
 * - fields: conceptual value types with disambiguation suffixes
 * 
 * <p>Key design goals:
 * - Scales with API size (O(n) where n = number of endpoints)
 * - Produces deterministic output
 * - Keeps vocabulary minimal for high cache hit rate
 * - NO table-column relationships (flat structure)
 */
public class Lexifier {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(Lexifier.class);

  private final OneMcp oneMcp;
  private final Path cacheDir;
  
  // HTTP method → canonical action mapping
  private static final Map<String, String> METHOD_TO_ACTION = Map.of(
      "get", "search",
      "post", "create",
      "put", "update",
      "patch", "update",
      "delete", "delete"
  );
  
  
  public Lexifier(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    this.cacheDir = determineCacheDirectory();
  }
  
  /**
   * Determine cache directory using same priority as ExecutionPlanCache:
   * 1. ONEMCP_CACHE_DIR environment variable
   * 2. ONEMCP_HOME_DIR/cache
   * 3. User home/.onemcp/cache
   */
  private Path determineCacheDirectory() {
    // Priority 1: ONEMCP_CACHE_DIR environment variable
    String cacheDirEnv = System.getenv("ONEMCP_CACHE_DIR");
    if (cacheDirEnv != null && !cacheDirEnv.isBlank()) {
      Path cacheDir = Path.of(cacheDirEnv);
      log.debug("Using cache directory from ONEMCP_CACHE_DIR: {}", cacheDir);
      return cacheDir;
    }
    
    // Priority 2: ONEMCP_HOME_DIR/cache
    String homeDir = System.getenv("ONEMCP_HOME_DIR");
    if (homeDir != null && !homeDir.isBlank()) {
      Path cacheDir = Path.of(homeDir, "cache");
      log.debug("Using cache directory from ONEMCP_HOME_DIR: {}", cacheDir);
      return cacheDir;
    }
    
    // Priority 3: User home/.onemcp/cache (fallback)
    String userHome = System.getProperty("user.home");
    if (userHome != null && !userHome.isBlank()) {
      Path cacheDir = Path.of(userHome, ".onemcp", "cache");
      log.debug("Using cache directory from user home: {}", cacheDir);
      return cacheDir;
    }
    
    // Final fallback: config file (for backward compatibility)
    String cacheLocation = oneMcp.configuration().getString("llm.cache.location", null);
    if (cacheLocation != null && !cacheLocation.isBlank()) {
      Path cacheDir = Path.of(cacheLocation);
      log.debug("Using cache directory from config: {}", cacheDir);
      return cacheDir;
    }
    
    // Ultimate fallback: temp directory
    Path cacheDir = Path.of(System.getProperty("java.io.tmpdir"), "onemcp", "cache");
    log.warn("Could not determine cache directory, using fallback: {}", cacheDir);
    return cacheDir;
  }
  
  /**
   * Extract conceptual lexicon from API specification using 2-pass LLM approach.
   * 
   * Pass 1: Create summary and extract conceptual actions/entities
   * Pass 2: Scan parameters and extract conceptual fields
   * 
   * @return ConceptualLexicon with actions, entities, and fields
   * @throws ExecutionException if extraction fails
   */
  public ConceptualLexicon extractLexicon() throws ExecutionException, HandbookException {
    log.info("Starting lexicon extraction (2-pass LLM)");
    long startTime = System.currentTimeMillis();
    
    // Start logging
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().startLexifierReport();
    }
    
    ConceptualLexicon lexicon = new ConceptualLexicon();
    
    try {
      // Load OpenAPI spec
      JsonNode openApiSpec = loadOpenApiSpec();
      
      // Pass 1: Create summary and extract actions/entities
      log.info("Pass 1: Creating summary and extracting actions/entities");
      extractActionsAndEntities(openApiSpec, lexicon);
      logPhase("summary-extraction", "Extracted " + lexicon.getActions().size() + " actions, " + 
          lexicon.getEntities().size() + " entities");
      
      // Pass 2: Extract fields from parameters
      log.info("Pass 2: Extracting fields from parameters");
      extractFieldsFromParameters(openApiSpec, lexicon);
      logPhase("field-extraction", "Extracted " + lexicon.getFields().size() + " fields");
      
      // Pass 3: Disambiguate and merge similar terms
      log.info("Pass 3: Disambiguating and merging similar terms");
      disambiguateLexicon(lexicon);
      logPhase("disambiguation", "Final lexicon: " + lexicon.getActions().size() + " actions, " + 
          lexicon.getEntities().size() + " entities, " + lexicon.getFields().size() + " fields");
      
      // Sort for deterministic output
      lexicon.sort();
      
      // Log final lexicon
      if (oneMcp.inferenceLogger() != null) {
        Map<String, Object> lexiconData = new HashMap<>();
        lexiconData.put("actions", lexicon.getActions());
        lexiconData.put("entities", lexicon.getEntities());
        lexiconData.put("fields", lexicon.getFields());
        oneMcp.inferenceLogger().logLexifierPhase("lexicon-final", 
            "Final lexicon: " + lexicon.getActions().size() + " actions, " + 
            lexicon.getEntities().size() + " entities, " + lexicon.getFields().size() + " fields",
            lexiconData);
      }
      
      // Save lexicon
      saveLexicon(lexicon);
      
      long duration = System.currentTimeMillis() - startTime;
      log.info("Lexicon extraction complete in {}ms: {}", duration, lexicon);
      
      // End logging
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().endLexifierReport(true, null);
      }
      
      return lexicon;
      
    } catch (Exception e) {
      log.error("Lexicon extraction failed", e);
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().endLexifierReport(false, e.getMessage());
      }
      throw new ExecutionException("Lexicon extraction failed: " + e.getMessage(), e);
    }
  }
  
  /**
   * Pass 1: Use LLM to create summary and extract actions/entities.
   */
  private void extractActionsAndEntities(JsonNode openApiSpec, ConceptualLexicon lexicon) 
      throws ExecutionException {
    try {
      // Create a simplified summary of the OpenAPI spec for the LLM
      Map<String, Object> summaryData = createApiSummary(openApiSpec);
      
      // Serialize to JSON
      String openApiSummaryJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(summaryData);
      
      // Log summary size for debugging
      log.info("OpenAPI summary size: {} characters", openApiSummaryJson.length());
      
      // Aggressively truncate if too large (many LLM providers have token limits)
      int maxSummarySize = 50000; // Conservative limit
      if (openApiSummaryJson.length() > maxSummarySize) {
        log.warn("OpenAPI summary is very large ({} chars), truncating to {} chars", 
            openApiSummaryJson.length(), maxSummarySize);
        
        // Truncate paths more aggressively
        if (summaryData.containsKey("paths")) {
          @SuppressWarnings("unchecked")
          Map<String, Object> paths = (Map<String, Object>) summaryData.get("paths");
          if (paths != null) {
            int maxPaths = 30; // Limit to 30 paths
            if (paths.size() > maxPaths) {
              log.warn("Too many paths ({}), truncating to first {}", paths.size(), maxPaths);
              Map<String, Object> truncatedPaths = new HashMap<>();
              int count = 0;
              for (Map.Entry<String, Object> entry : paths.entrySet()) {
                if (count++ >= maxPaths) break;
                truncatedPaths.put(entry.getKey(), entry.getValue());
              }
              summaryData.put("paths", truncatedPaths);
            }
          }
        }
        
        // Re-serialize and check size again
        openApiSummaryJson = JacksonUtility.getJsonMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(summaryData);
        
        // If still too large, truncate the JSON string itself (last resort)
        if (openApiSummaryJson.length() > maxSummarySize) {
          log.warn("Summary still too large after path truncation, truncating JSON string to {} chars", maxSummarySize);
          openApiSummaryJson = openApiSummaryJson.substring(0, maxSummarySize) + "...\"}";
          // Try to make it valid JSON
          if (!openApiSummaryJson.endsWith("}")) {
            // Find last complete JSON structure
            int lastBrace = openApiSummaryJson.lastIndexOf('}');
            if (lastBrace > 0) {
              openApiSummaryJson = openApiSummaryJson.substring(0, lastBrace + 1);
            }
          }
        }
        
        log.info("Final summary size after truncation: {} characters", openApiSummaryJson.length());
      }
      
      // Validate JSON is valid
      try {
        JacksonUtility.getJsonMapper().readTree(openApiSummaryJson);
      } catch (Exception e) {
        throw new ExecutionException("Generated OpenAPI summary is not valid JSON: " + e.getMessage(), e);
      }
      
      // Get prompt template
      com.gentoro.onemcp.prompt.PromptRepository promptRepository = oneMcp.promptRepository();
      com.gentoro.onemcp.prompt.PromptTemplate template = promptRepository.get("lexifier-summary");
      if (template == null) {
        throw new ExecutionException("Prompt template 'lexifier-summary' not found");
      }
      
      // Build messages
      com.gentoro.onemcp.prompt.PromptTemplate.PromptSession session = template.newSession();
      session.enable("lexifier-summary", Map.of("openapi_spec", openApiSummaryJson));
      List<com.gentoro.onemcp.model.LlmClient.Message> messages = session.renderMessages();
      
      if (messages.isEmpty()) {
        throw new ExecutionException("No messages rendered for 'lexifier-summary'");
      }
      
      // Check message sizes and log details
      int totalMessageSize = 0;
      for (com.gentoro.onemcp.model.LlmClient.Message msg : messages) {
        totalMessageSize += msg.content().length();
      }
      log.info("Lexifier summary: {} messages, total size: {} chars", messages.size(), totalMessageSize);
      if (totalMessageSize > 200000) {
        log.warn("Total message size is very large ({} chars), may cause LLM issues", totalMessageSize);
      }
      
      // Log input messages and start inference
      long startTime = System.currentTimeMillis();
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logLlmInputMessages(messages);
        oneMcp.inferenceLogger().logLlmInferenceStart("lexifier-summary");
      }
      
      // Get temperature
      Float temperature = template.temperature().orElse(0.3f);
      
      // Call LLM with telemetry
      com.gentoro.onemcp.model.LlmClient llmClient = oneMcp.llmClient();
      if (llmClient == null) {
        throw new ExecutionException("LLM client not available");
      }
      
      // Create telemetry sink to capture token usage
      final long[] promptTokens = {0};
      final long[] completionTokens = {0};
      
      com.gentoro.onemcp.model.LlmClient.TelemetrySink tokenSink =
          new com.gentoro.onemcp.model.LlmClient.TelemetrySink() {
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
              attrs.put("phase", "lexifier-summary");
              return attrs;
            }
          };
      
      String response;
      try (com.gentoro.onemcp.model.LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
        // Pass null for listener (not all LLM implementations support it)
        response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, temperature);
      } catch (Exception e) {
        // Log detailed error information
        log.error("LLM call failed for lexifier summary");
        log.error("Messages count: {}", messages.size());
        if (!messages.isEmpty()) {
          for (int i = 0; i < messages.size(); i++) {
            String msgContent = messages.get(i).content();
            log.error("Message {} ({}): size={} chars", 
                i, messages.get(i).role(), msgContent.length());
            if (i == 0) {
              log.error("First message preview (first 1000 chars): {}", 
                  msgContent.length() > 1000 ? msgContent.substring(0, 1000) + "..." : msgContent);
            }
          }
          log.error("Total input size: {} characters", 
              messages.stream().mapToInt(m -> m.content().length()).sum());
        }
        log.error("Exception type: {}", e.getClass().getName());
        log.error("Exception message: {}", e.getMessage());
        if (e.getCause() != null) {
          log.error("Cause type: {}", e.getCause().getClass().getName());
          log.error("Cause message: {}", e.getCause().getMessage());
          if (e.getCause().getCause() != null) {
            log.error("Root cause type: {}", e.getCause().getCause().getClass().getName());
            log.error("Root cause message: {}", e.getCause().getCause().getMessage());
          }
        }
        log.error("Full stack trace:", e);
        
        // Build comprehensive error message
        StringBuilder errorMsg = new StringBuilder("LLM call failed: " + e.getMessage());
        Throwable cause = e.getCause();
        int depth = 0;
        while (cause != null && depth < 3) {
          errorMsg.append(" (Caused by: ").append(cause.getClass().getSimpleName());
          if (cause.getMessage() != null && !cause.getMessage().isEmpty()) {
            errorMsg.append(": ").append(cause.getMessage());
          }
          errorMsg.append(")");
          cause = cause.getCause();
          depth++;
        }
        throw new ExecutionException(errorMsg.toString(), e);
      }
      
      long duration = System.currentTimeMillis() - startTime;
      
      // Log inference completion
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logLlmInferenceComplete(
            "lexifier-summary", duration, promptTokens[0], completionTokens[0], response);
      }
      
      if (response == null || response.trim().isEmpty()) {
        throw new ExecutionException("LLM returned empty response");
      }
      
      // Parse response
      String jsonContent = extractJsonFromResponse(response);
      if (jsonContent == null || jsonContent.trim().isEmpty()) {
        log.error("Failed to extract JSON from LLM response. Response: {}", 
            response.substring(0, Math.min(500, response.length())));
        throw new ExecutionException("Failed to extract JSON from LLM response");
      }
      
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      JsonNode result;
      try {
        result = mapper.readTree(jsonContent);
      } catch (Exception e) {
        log.error("Failed to parse LLM response as JSON. Response: {}", 
            jsonContent.substring(0, Math.min(500, jsonContent.length())));
        throw new ExecutionException("Failed to parse LLM response as JSON: " + e.getMessage(), e);
      }
      
      // Extract actions
      if (result.has("actions") && result.get("actions").isArray()) {
        for (JsonNode action : result.get("actions")) {
          lexicon.addAction(action.asText());
        }
      }
      
      // Extract entities
      if (result.has("entities") && result.get("entities").isArray()) {
        for (JsonNode entity : result.get("entities")) {
          lexicon.addEntity(entity.asText());
        }
      }
      
      // Extract candidate fields (initial list from summary)
      if (result.has("candidateFields") && result.get("candidateFields").isArray()) {
        for (JsonNode field : result.get("candidateFields")) {
          lexicon.addField(field.asText());
        }
      }
      
      // Extract entity-to-endpoint mappings and save endpoint index files
      if (result.has("entityEndpoints") && result.get("entityEndpoints").isObject()) {
        JsonNode entityEndpoints = result.get("entityEndpoints");
        entityEndpoints.fields().forEachRemaining(entry -> {
          String entity = entry.getKey();
          JsonNode operationIds = entry.getValue();
          if (operationIds.isArray()) {
            List<String> opIds = new ArrayList<>();
            for (JsonNode opId : operationIds) {
              opIds.add(opId.asText());
            }
            // Extract full endpoint details and save
            saveEntityEndpoints(entity, opIds, openApiSpec);
          }
        });
      }
      
      log.info("Pass 1 complete: {} actions, {} entities, {} candidate fields", 
          lexicon.getActions().size(), lexicon.getEntities().size(), lexicon.getFields().size());
      
    } catch (Exception e) {
      throw new ExecutionException("Failed to extract actions/entities: " + e.getMessage(), e);
    }
  }
  
  /**
   * Pass 2: Use LLM to extract fields from parameters and schemas.
   */
  private void extractFieldsFromParameters(JsonNode openApiSpec, ConceptualLexicon lexicon) 
      throws ExecutionException {
    try {
      // Extract all parameters and schema properties
      Map<String, Object> parametersData = extractParametersAndSchemas(openApiSpec);
      
      // Serialize to JSON
      String parametersJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(parametersData);
      
      // Get prompt template
      com.gentoro.onemcp.prompt.PromptRepository promptRepository = oneMcp.promptRepository();
      com.gentoro.onemcp.prompt.PromptTemplate template = promptRepository.get("lexifier-fields");
      if (template == null) {
        throw new ExecutionException("Prompt template 'lexifier-fields' not found");
      }
      
      // Build messages
      com.gentoro.onemcp.prompt.PromptTemplate.PromptSession session = template.newSession();
      session.enable("lexifier-fields", Map.of("parameters_and_schemas", parametersJson));
      List<com.gentoro.onemcp.model.LlmClient.Message> messages = session.renderMessages();
      
      if (messages.isEmpty()) {
        throw new ExecutionException("No messages rendered for 'lexifier-fields'");
      }
      
      // Log input messages and start inference
      long startTime = System.currentTimeMillis();
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logLlmInputMessages(messages);
        oneMcp.inferenceLogger().logLlmInferenceStart("lexifier-fields");
      }
      
      // Get temperature
      Float temperature = template.temperature().orElse(0.3f);
      
      // Call LLM with telemetry
      com.gentoro.onemcp.model.LlmClient llmClient = oneMcp.llmClient();
      if (llmClient == null) {
        throw new ExecutionException("LLM client not available");
      }
      
      // Create telemetry sink to capture token usage
      final long[] promptTokens = {0};
      final long[] completionTokens = {0};
      
      com.gentoro.onemcp.model.LlmClient.TelemetrySink tokenSink =
          new com.gentoro.onemcp.model.LlmClient.TelemetrySink() {
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
              attrs.put("phase", "lexifier-fields");
              return attrs;
            }
          };
      
      String response;
      try (com.gentoro.onemcp.model.LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
        response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, temperature);
      } catch (Exception e) {
        log.error("LLM call failed for lexifier fields", e);
        throw new ExecutionException("LLM call failed: " + e.getMessage(), e);
      }
      
      long duration = System.currentTimeMillis() - startTime;
      
      // Log inference completion
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logLlmInferenceComplete(
            "lexifier-fields", duration, promptTokens[0], completionTokens[0], response);
      }
      
      if (response == null || response.trim().isEmpty()) {
        throw new ExecutionException("LLM returned empty response");
      }
      
      // Parse response
      String jsonContent = extractJsonFromResponse(response);
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      JsonNode result = mapper.readTree(jsonContent);
      
      // Extract fields and clean them up
      if (result.has("fields") && result.get("fields").isArray()) {
        for (JsonNode field : result.get("fields")) {
          String fieldName = field.asText();
          // Clean up field names: remove type suffixes, fix date fields
          fieldName = cleanFieldName(fieldName);
          if (fieldName != null && !fieldName.isEmpty()) {
            lexicon.addField(fieldName);
          }
        }
      }
      
      log.info("Pass 2 complete: {} fields", lexicon.getFields().size());
      
    } catch (Exception e) {
      throw new ExecutionException("Failed to extract fields: " + e.getMessage(), e);
    }
  }
  
  /**
   * Pass 3: Disambiguate and merge similar terms using LLM.
   */
  private void disambiguateLexicon(ConceptualLexicon lexicon) throws ExecutionException {
    try {
      // Serialize current lexicon
      Map<String, Object> lexiconData = new HashMap<>();
      lexiconData.put("actions", lexicon.getActions());
      lexiconData.put("entities", lexicon.getEntities());
      lexiconData.put("fields", lexicon.getFields());
      
      String lexiconJson = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(lexiconData);
      
      // Get prompt template
      com.gentoro.onemcp.prompt.PromptRepository promptRepository = oneMcp.promptRepository();
      com.gentoro.onemcp.prompt.PromptTemplate template = promptRepository.get("lexifier-disambiguate");
      if (template == null) {
        throw new ExecutionException("Prompt template 'lexifier-disambiguate' not found");
      }
      
      // Build messages
      com.gentoro.onemcp.prompt.PromptTemplate.PromptSession session = template.newSession();
      session.enable("lexifier-disambiguate", Map.of("lexicon", lexiconJson));
      List<com.gentoro.onemcp.model.LlmClient.Message> messages = session.renderMessages();
      
      if (messages.isEmpty()) {
        throw new ExecutionException("No messages rendered for 'lexifier-disambiguate'");
      }
      
      // Log input messages and start inference
      long startTime = System.currentTimeMillis();
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logLlmInputMessages(messages);
        oneMcp.inferenceLogger().logLlmInferenceStart("lexifier-disambiguate");
      }
      
      // Get temperature
      Float temperature = template.temperature().orElse(0.3f);
      
      // Call LLM with telemetry
      com.gentoro.onemcp.model.LlmClient llmClient = oneMcp.llmClient();
      if (llmClient == null) {
        throw new ExecutionException("LLM client not available");
      }
      
      // Create telemetry sink to capture token usage
      final long[] promptTokens = {0};
      final long[] completionTokens = {0};
      
      com.gentoro.onemcp.model.LlmClient.TelemetrySink tokenSink =
          new com.gentoro.onemcp.model.LlmClient.TelemetrySink() {
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
              attrs.put("phase", "lexifier-disambiguate");
              return attrs;
            }
          };
      
      String response;
      try (com.gentoro.onemcp.model.LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
        response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, temperature);
      } catch (Exception e) {
        log.error("LLM call failed for lexifier disambiguation", e);
        throw new ExecutionException("LLM call failed: " + e.getMessage(), e);
      }
      
      long duration = System.currentTimeMillis() - startTime;
      
      // Log inference completion
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().logLlmInferenceComplete(
            "lexifier-disambiguate", duration, promptTokens[0], completionTokens[0], response);
      }
      
      if (response == null || response.trim().isEmpty()) {
        throw new ExecutionException("LLM returned empty response");
      }
      
      // Parse response
      String jsonContent = extractJsonFromResponse(response);
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      JsonNode result = mapper.readTree(jsonContent);
      
      // Replace lexicon with disambiguated version
      lexicon.getActions().clear();
      lexicon.getEntities().clear();
      lexicon.getFields().clear();
      
      // Extract disambiguated actions
      if (result.has("actions") && result.get("actions").isArray()) {
        for (JsonNode action : result.get("actions")) {
          lexicon.addAction(action.asText());
        }
      }
      
      // Extract disambiguated entities
      if (result.has("entities") && result.get("entities").isArray()) {
        for (JsonNode entity : result.get("entities")) {
          lexicon.addEntity(entity.asText());
        }
      }
      
      // Extract disambiguated fields and clean them
      if (result.has("fields") && result.get("fields").isArray()) {
        for (JsonNode field : result.get("fields")) {
          String fieldName = field.asText();
          // Clean up field names: remove type suffixes, fix date fields
          fieldName = cleanFieldName(fieldName);
          if (fieldName != null && !fieldName.isEmpty()) {
            lexicon.addField(fieldName);
          }
        }
      }
      
      log.info("Pass 3 complete: {} actions, {} entities, {} fields", 
          lexicon.getActions().size(), lexicon.getEntities().size(), lexicon.getFields().size());
      
    } catch (Exception e) {
      throw new ExecutionException("Failed to disambiguate lexicon: " + e.getMessage(), e);
    }
  }
  
  /**
   * Create a simplified summary of the OpenAPI spec for Pass 1.
   */
  private Map<String, Object> createApiSummary(JsonNode openApiSpec) {
    Map<String, Object> summary = new HashMap<>();
    
    // Include basic info
    if (openApiSpec.has("info")) {
      summary.put("info", openApiSpec.get("info"));
    }
    
    // Include paths (simplified - just method and operationId)
    Map<String, Object> pathsSummary = new HashMap<>();
    if (openApiSpec.has("paths")) {
      openApiSpec.get("paths").fields().forEachRemaining(pathEntry -> {
        String path = pathEntry.getKey();
        Map<String, Object> methods = new HashMap<>();
        pathEntry.getValue().fields().forEachRemaining(methodEntry -> {
          String method = methodEntry.getKey();
          JsonNode operation = methodEntry.getValue();
          Map<String, Object> opSummary = new HashMap<>();
          if (operation.has("operationId")) {
            opSummary.put("operationId", operation.get("operationId").asText());
          }
          if (operation.has("summary")) {
            opSummary.put("summary", operation.get("summary").asText());
          }
          methods.put(method, opSummary);
        });
        pathsSummary.put(path, methods);
      });
    }
    summary.put("paths", pathsSummary);
    
    // Include schema names (not full schemas)
    if (openApiSpec.has("components")) {
      JsonNode components = openApiSpec.get("components");
      if (components.has("schemas")) {
        List<String> schemaNames = new ArrayList<>();
        components.get("schemas").fieldNames().forEachRemaining(schemaNames::add);
        summary.put("schemaNames", schemaNames);
      }
    }
    
    return summary;
  }
  
  /**
   * Extract endpoint parameters and responses for Pass 2 (batched).
   * Resolves $ref references to extract actual field names, especially from enum values.
   */
  private Map<String, Object> extractParametersAndSchemas(JsonNode openApiSpec) {
    Map<String, Object> data = new HashMap<>();
    
    // Batch all endpoint data: parameters, request bodies, and responses
    List<Map<String, Object>> endpoints = new ArrayList<>();
    if (openApiSpec.has("paths")) {
      openApiSpec.get("paths").fields().forEachRemaining(pathEntry -> {
        String path = pathEntry.getKey();
        pathEntry.getValue().fields().forEachRemaining(methodEntry -> {
          String method = methodEntry.getKey();
          JsonNode operation = methodEntry.getValue();
          
          Map<String, Object> endpoint = new HashMap<>();
          endpoint.put("path", path);
          endpoint.put("method", method);
          if (operation.has("operationId")) {
            endpoint.put("operationId", operation.get("operationId").asText());
          }
          
          // Extract parameters
          List<Map<String, Object>> params = new ArrayList<>();
          if (operation.has("parameters")) {
            operation.get("parameters").forEach(param -> {
              Map<String, Object> paramData = new HashMap<>();
              if (param.has("name")) paramData.put("name", param.get("name").asText());
              if (param.has("in")) paramData.put("in", param.get("in").asText());
              if (param.has("schema")) {
                paramData.put("schema", extractSchemaInfoResolved(param.get("schema"), openApiSpec));
              }
              params.add(paramData);
            });
          }
          endpoint.put("parameters", params);
          
          // Extract request body
          if (operation.has("requestBody")) {
            JsonNode requestBody = operation.get("requestBody");
            if (requestBody.has("content")) {
              JsonNode content = requestBody.get("content");
              if (content.has("application/json")) {
                JsonNode jsonContent = content.get("application/json");
                if (jsonContent.has("schema")) {
                  endpoint.put("requestBody", extractSchemaInfoResolved(jsonContent.get("schema"), openApiSpec));
                }
              }
            }
          }
          
          // Extract responses
          Map<String, Object> responses = new HashMap<>();
          if (operation.has("responses")) {
            operation.get("responses").fields().forEachRemaining(responseEntry -> {
              String statusCode = responseEntry.getKey();
              JsonNode response = responseEntry.getValue();
              if (response.has("content")) {
                JsonNode content = response.get("content");
                if (content.has("application/json")) {
                  JsonNode jsonContent = content.get("application/json");
                  if (jsonContent.has("schema")) {
                    responses.put(statusCode, extractSchemaInfoResolved(jsonContent.get("schema"), openApiSpec));
                  }
                }
              }
            });
          }
          endpoint.put("responses", responses);
          
          endpoints.add(endpoint);
        });
      });
    }
    data.put("endpoints", endpoints);
    
    return data;
  }
  
  /**
   * Extract schema information (properties and types) without full recursion.
   * Resolves $ref references to get actual schema content.
   */
  private Map<String, Object> extractSchemaInfoResolved(JsonNode schema, JsonNode openApiSpec) {
    // If it's a $ref, resolve it first
    if (schema.has("$ref")) {
      String ref = schema.get("$ref").asText();
      if (ref.startsWith("#/components/schemas/")) {
        String schemaName = ref.substring("#/components/schemas/".length());
        JsonNode components = openApiSpec.get("components");
        if (components != null && components.has("schemas")) {
          JsonNode schemas = components.get("schemas");
          if (schemas.has(schemaName)) {
            return extractSchemaInfoResolved(schemas.get(schemaName), openApiSpec);
          }
        }
      }
    }
    
    Map<String, Object> info = new HashMap<>();
    
    if (schema.has("type")) {
      info.put("type", schema.get("type").asText());
    }
    if (schema.has("format")) {
      info.put("format", schema.get("format").asText());
    }
    
    // Extract enum values - these often contain field names like "customer.state"
    if (schema.has("enum")) {
      List<String> enumValues = new ArrayList<>();
      schema.get("enum").forEach(e -> enumValues.add(e.asText()));
      info.put("enum", enumValues);
    }
    
    // Extract properties (first level only to keep it manageable)
    if (schema.has("properties")) {
      Map<String, Object> properties = new HashMap<>();
      schema.get("properties").fields().forEachRemaining(propEntry -> {
        String propName = propEntry.getKey();
        JsonNode propSchema = propEntry.getValue();
        Map<String, Object> propInfo = extractSchemaInfoResolved(propSchema, openApiSpec);
        properties.put(propName, propInfo);
      });
      info.put("properties", properties);
    }
    
    // Extract items for arrays
    if (schema.has("items")) {
      info.put("items", extractSchemaInfoResolved(schema.get("items"), openApiSpec));
    }
    
    // Extract allOf, anyOf, oneOf
    if (schema.has("allOf")) {
      List<Map<String, Object>> allOf = new ArrayList<>();
      schema.get("allOf").forEach(item -> allOf.add(extractSchemaInfoResolved(item, openApiSpec)));
      info.put("allOf", allOf);
    }
    
    return info;
  }
  
  /**
   * Extract schema information (properties and types) without full recursion.
   * Legacy method for backward compatibility - doesn't resolve $ref.
   */
  private Map<String, Object> extractSchemaInfo(JsonNode schema) {
    Map<String, Object> info = new HashMap<>();
    
    if (schema.has("type")) {
      info.put("type", schema.get("type").asText());
    }
    if (schema.has("format")) {
      info.put("format", schema.get("format").asText());
    }
    if (schema.has("$ref")) {
      info.put("$ref", schema.get("$ref").asText());
    }
    
    // Extract properties (first level only to keep it manageable)
    if (schema.has("properties")) {
      Map<String, Object> properties = new HashMap<>();
      schema.get("properties").fields().forEachRemaining(propEntry -> {
        String propName = propEntry.getKey();
        JsonNode propSchema = propEntry.getValue();
        Map<String, Object> propInfo = new HashMap<>();
        if (propSchema.has("type")) propInfo.put("type", propSchema.get("type").asText());
        if (propSchema.has("format")) propInfo.put("format", propSchema.get("format").asText());
        properties.put(propName, propInfo);
      });
      info.put("properties", properties);
    }
    
    return info;
  }
  
  /**
   * Extract JSON from LLM response (handles markdown code blocks).
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
  
  private void logPhase(String phase, String message) {
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logLexifierPhase(phase, message, null);
    }
  }
  
  /**
   * Load OpenAPI specification from handbook.
   */
  private JsonNode loadOpenApiSpec() throws ExecutionException, HandbookException {
    try {
      Path handbookPath = oneMcp.handbook().location();
      if (handbookPath == null) {
        throw new ExecutionException("Handbook location is not available");
      }
      
      // Look for OpenAPI files in handbook
      Path apisDir = handbookPath.resolve("apis");
      Path openapiDir = handbookPath.resolve("openapi");
      Path sourceDir = null;
      
      if (Files.exists(apisDir)) {
        sourceDir = apisDir;
      } else if (Files.exists(openapiDir)) {
        sourceDir = openapiDir;
      } else {
        throw new ExecutionException("APIs directory not found in: " + handbookPath);
      }
      
      // Find first OpenAPI spec file
      Path specFile = null;
      try (var stream = Files.list(sourceDir)) {
        specFile = stream
            .filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml") || p.toString().endsWith(".json"))
            .findFirst()
            .orElse(null);
      }
      
      if (specFile == null) {
        throw new ExecutionException("No OpenAPI spec file found in: " + sourceDir);
      }
      
      String content = Files.readString(specFile);
      log.info("Loaded OpenAPI spec from: {}", specFile);
      
      // Parse based on file extension
      if (specFile.toString().endsWith(".json")) {
        return JacksonUtility.getJsonMapper().readTree(content);
      } else {
        return JacksonUtility.getYamlMapper().readTree(content);
      }
    } catch (IOException e) {
      throw new ExecutionException("Failed to load OpenAPI spec: " + e.getMessage(), e);
    }
  }
  
  /**
   * Extract canonical actions from HTTP methods.
   */
  private void extractActions(JsonNode spec, ConceptualLexicon lexicon) {
    JsonNode paths = spec.get("paths");
    if (paths == null) return;
    
    Set<String> methods = new HashSet<>();
    paths.fields().forEachRemaining(pathEntry -> {
      pathEntry.getValue().fields().forEachRemaining(methodEntry -> {
        String method = methodEntry.getKey().toLowerCase();
        if (METHOD_TO_ACTION.containsKey(method)) {
          methods.add(method);
        }
      });
    });
    
    // Map to canonical actions
    for (String method : methods) {
      String action = METHOD_TO_ACTION.get(method);
      if (action != null) {
        lexicon.addAction(action);
      }
    }
  }
  
  /**
   * Extract entities from path segments and schema names.
   */
  private void extractEntities(JsonNode spec, ConceptualLexicon lexicon) {
    Set<String> entities = new HashSet<>();
    
    // Extract from paths
    JsonNode paths = spec.get("paths");
    if (paths != null) {
      paths.fieldNames().forEachRemaining(path -> {
        // Extract entity names from path segments
        // e.g., /api/sales/{id} → "sale"
        // e.g., /customers/{customerId}/orders → "customer", "order"
        String[] segments = path.split("/");
        for (String segment : segments) {
          if (!segment.isEmpty() && !segment.startsWith("{") && !segment.equals("api") && !segment.equals("v1") && !segment.equals("v2")) {
            String entity = singularize(segment.toLowerCase());
            if (isValidEntityName(entity)) {
              entities.add(entity);
            }
          }
        }
      });
    }
    
    // Extract from schema names
    JsonNode components = spec.get("components");
    if (components != null) {
      JsonNode schemas = components.get("schemas");
      if (schemas != null) {
        schemas.fieldNames().forEachRemaining(schemaName -> {
          String entity = singularize(schemaName.toLowerCase());
          if (isValidEntityName(entity)) {
            entities.add(entity);
          }
        });
      }
    }
    
    entities.forEach(lexicon::addEntity);
  }
  
  /**
   * Clean field name to comply with DTN rules: remove type suffixes, fix date fields.
   * 
   * Rules:
   * - Remove `_int` and `_decimal` type suffixes (DTN fields are all strings)
   * - Convert `date_year_int` → `date_yyyy`
   * - Convert `date_month_int` → `date_mm` or `date_yyyy_mm`
   * - Convert `date_week_int` → `date_week`
   * - Remove entity prefixes (e.g., `sale_date` → `date`)
   */
  private String cleanFieldName(String fieldName) {
    if (fieldName == null || fieldName.isEmpty()) {
      return fieldName;
    }
    
    String cleaned = fieldName;
    
    // Fix date fields with type suffixes
    if (cleaned.equals("date_year_int") || cleaned.equals("dateYearInt")) {
      return "date_yyyy";
    }
    if (cleaned.equals("date_month_int") || cleaned.equals("dateMonthInt")) {
      return "date_mm"; // or date_yyyy_mm depending on context
    }
    if (cleaned.equals("date_week_int") || cleaned.equals("dateWeekInt")) {
      return "date_week";
    }
    if (cleaned.equals("date_day_int") || cleaned.equals("dateDayInt")) {
      return "date_dd";
    }
    
    // Remove type suffixes (_int, _decimal) - DTN fields are all strings
    cleaned = cleaned.replaceAll("_int$", "");
    cleaned = cleaned.replaceAll("_decimal$", "");
    cleaned = cleaned.replaceAll("Int$", "");
    cleaned = cleaned.replaceAll("Decimal$", "");
    
    // Remove entity prefixes for date fields (sale_date_yyyy_mm_dd → date_yyyy_mm_dd)
    if (cleaned.startsWith("sale_date_") || cleaned.startsWith("customer_date_") || 
        cleaned.startsWith("product_date_")) {
      cleaned = cleaned.replaceFirst("^(sale|customer|product)_date_", "date_");
    }
    
    // Remove entity prefixes for other common fields
    // But keep them for disambiguation (customer_id vs product_id)
    // This is a heuristic - be conservative
    
    return cleaned;
  }
  
  /**
   * Singularize a word (simple heuristic).
   */
  private String singularize(String word) {
    if (word == null) return null;
    if (word.endsWith("ies")) return word.substring(0, word.length() - 3) + "y";
    if (word.endsWith("es") && !word.endsWith("ases")) return word.substring(0, word.length() - 2);
    if (word.endsWith("s") && !word.endsWith("ss")) return word.substring(0, word.length() - 1);
    return word;
  }
  
  /**
   * Check if a string is a valid entity name.
   */
  private boolean isValidEntityName(String name) {
    if (name == null || name.length() < 2) return false;
    if (name.matches("^(v\\d+|api|query|health|field|endpoint|schema|spec)$")) return false;
    return name.matches("^[a-z][a-z0-9_]*$");
  }
  
  /**
   * Save lexicon to cache directory.
   */
  private void saveLexicon(ConceptualLexicon lexicon) throws IOException {
    Files.createDirectories(cacheDir);
    Path lexiconPath = cacheDir.resolve("lexicon.json");
    
    ObjectMapper mapper = JacksonUtility.getJsonMapper();
    String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(lexicon);
    Files.writeString(lexiconPath, json);
    
    log.info("Saved lexicon to: {}", lexiconPath);
  }
  
  /**
   * Get the cache directory path.
   * @return the cache directory
   */
  public Path getCacheDirectory() {
    return cacheDir;
  }
  
  /**
   * Load lexicon from cache.
   */
  public ConceptualLexicon loadLexicon() throws ExecutionException {
    Path lexiconPath = cacheDir.resolve("lexicon.json");
    if (!Files.exists(lexiconPath)) {
      throw new ExecutionException("Lexicon not found. Run 'index' command first.");
    }
    
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      return mapper.readValue(Files.readString(lexiconPath), ConceptualLexicon.class);
    } catch (Exception e) {
      throw new ExecutionException("Failed to load lexicon: " + e.getMessage(), e);
    }
  }
  
  /**
   * Extract full endpoint details for an entity and save to endpoint_info/<entity>.json.
   */
  private void saveEntityEndpoints(String entity, List<String> operationIds, JsonNode openApiSpec) {
    try {
      List<TableEndpointIndex.EndpointInfo> endpoints = new ArrayList<>();
      
      // Find all endpoints with matching operationIds
      if (openApiSpec.has("paths")) {
        openApiSpec.get("paths").fields().forEachRemaining(pathEntry -> {
          String path = pathEntry.getKey();
          pathEntry.getValue().fields().forEachRemaining(methodEntry -> {
            String method = methodEntry.getKey().toUpperCase();
            JsonNode operation = methodEntry.getValue();
            
            if (operation.has("operationId")) {
              String operationId = operation.get("operationId").asText();
              if (operationIds.contains(operationId)) {
                TableEndpointIndex.EndpointInfo endpointInfo = new TableEndpointIndex.EndpointInfo();
                endpointInfo.setMethod(method);
                endpointInfo.setPath(path);
                endpointInfo.setOperationId(operationId);
                
                if (operation.has("summary")) {
                  endpointInfo.setSummary(operation.get("summary").asText());
                }
                if (operation.has("description")) {
                  endpointInfo.setDescription(operation.get("description").asText());
                }
                
                // Extract parameters
                Map<String, Object> params = new HashMap<>();
                if (operation.has("parameters")) {
                  List<Map<String, Object>> paramList = new ArrayList<>();
                  operation.get("parameters").forEach(param -> {
                    Map<String, Object> paramData = new HashMap<>();
                    if (param.has("name")) paramData.put("name", param.get("name").asText());
                    if (param.has("in")) paramData.put("in", param.get("in").asText());
                    if (param.has("schema")) {
                      paramData.put("schema", extractSchemaInfo(param.get("schema")));
                    }
                    paramList.add(paramData);
                  });
                  params.put("parameters", paramList);
                }
                endpointInfo.setParams(params);
                
                // Extract request schema
                if (operation.has("requestBody")) {
                  JsonNode requestBody = operation.get("requestBody");
                  if (requestBody.has("content")) {
                    JsonNode content = requestBody.get("content");
                    if (content.has("application/json")) {
                      JsonNode jsonContent = content.get("application/json");
                      if (jsonContent.has("schema")) {
                        endpointInfo.setRequestSchema(extractSchemaInfo(jsonContent.get("schema")));
                      }
                    }
                  }
                }
                
                // Extract response schema (use 200 if available, otherwise first)
                if (operation.has("responses")) {
                  JsonNode responses = operation.get("responses");
                  if (responses.has("200")) {
                    JsonNode response200 = responses.get("200");
                    if (response200.has("content")) {
                      JsonNode content = response200.get("content");
                      if (content.has("application/json")) {
                        JsonNode jsonContent = content.get("application/json");
                        if (jsonContent.has("schema")) {
                          endpointInfo.setResponseSchema(extractSchemaInfo(jsonContent.get("schema")));
                        }
                      }
                    }
                  } else if (responses.size() > 0) {
                    // Use first response
                    JsonNode firstResponse = responses.elements().next();
                    if (firstResponse.has("content")) {
                      JsonNode content = firstResponse.get("content");
                      if (content.has("application/json")) {
                        JsonNode jsonContent = content.get("application/json");
                        if (jsonContent.has("schema")) {
                          endpointInfo.setResponseSchema(extractSchemaInfo(jsonContent.get("schema")));
                        }
                      }
                    }
                  }
                }
                
                endpoints.add(endpointInfo);
              }
            }
          });
        });
      }
      
      // Save to file
      if (!endpoints.isEmpty()) {
        TableEndpointIndex index = new TableEndpointIndex(entity, endpoints);
        Path endpointInfoDir = cacheDir.resolve("endpoint_info");
        Files.createDirectories(endpointInfoDir);
        Path indexFile = endpointInfoDir.resolve(entity + ".json");
        
        ObjectMapper mapper = JacksonUtility.getJsonMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(index);
        Files.writeString(indexFile, json);
        
        log.info("Saved endpoint index for entity '{}': {} endpoints -> {}", 
            entity, endpoints.size(), indexFile);
      } else {
        log.warn("No endpoints found for entity '{}' with operationIds: {}", entity, operationIds);
      }
      
    } catch (Exception e) {
      log.error("Failed to save endpoint index for entity '{}': {}", entity, e.getMessage(), e);
      // Don't throw - continue with other entities
    }
  }
  
  /**
   * Get endpoint information for an entity (used by planner).
   * 
   * @param entity the entity name
   * @return TableEndpointIndex with endpoint details, or null if not found
   */
  public TableEndpointIndex endpointInfo(String entity) throws ExecutionException {
    Path endpointInfoDir = cacheDir.resolve("endpoint_info");
    Path indexFile = endpointInfoDir.resolve(entity + ".json");
    
    if (!Files.exists(indexFile)) {
      log.debug("Endpoint index file not found for entity '{}': {}", entity, indexFile);
      return null;
    }
    
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      return mapper.readValue(Files.readString(indexFile), TableEndpointIndex.class);
    } catch (Exception e) {
      throw new ExecutionException("Failed to load endpoint index for entity '" + entity + "': " + e.getMessage(), e);
    }
  }
}

