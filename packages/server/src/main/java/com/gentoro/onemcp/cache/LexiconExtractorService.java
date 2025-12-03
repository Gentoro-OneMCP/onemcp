package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for extracting canonical lexicon from OpenAPI specifications.
 *
 * <p>Uses LLM to extract actions, entities, fields, operators, and aggregates from API specs
 * according to the Prompt Schema Specification.
 */
public class LexiconExtractorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(LexiconExtractorService.class);

  private final OneMcp oneMcp;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public LexiconExtractorService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Extract lexicon from OpenAPI specifications in the handbook.
   *
   * @param handbookPath path to the handbook directory
   * @return extracted lexicon
   * @throws IOException if file operations fail
   * @throws ExecutionException if LLM extraction fails
   */
  public PromptLexicon extractLexicon(Path handbookPath)
      throws IOException, ExecutionException {
    log.info("Extracting lexicon from handbook: {}", handbookPath);

    // Load OpenAPI files from apis/ directory (CLI convention)
    // Also check openapi/ directory (server convention) for backward compatibility
    Path apisDir = handbookPath.resolve("apis");
    Path openapiDir = handbookPath.resolve("openapi");

    Path sourceDir = null;
    if (Files.exists(apisDir)) {
      sourceDir = apisDir;
    } else if (Files.exists(openapiDir)) {
      sourceDir = openapiDir;
    } else {
      throw new IOException(
          "APIs directory not found. Expected either 'apis/' or 'openapi/' in: " + handbookPath);
    }

    List<Map<String, String>> openapiFiles = new ArrayList<>();
    Files.walk(sourceDir)
        .filter(Files::isRegularFile)
        .filter(
            p -> {
              String name = p.getFileName().toString().toLowerCase();
              return name.endsWith(".yaml") || name.endsWith(".yml") || name.endsWith(".json");
            })
        .forEach(
            file -> {
              try {
                Map<String, String> fileInfo = new HashMap<>();
                fileInfo.put("name", file.getFileName().toString());
                fileInfo.put("content", Files.readString(file));
                openapiFiles.add(fileInfo);
              } catch (Exception e) {
                log.warn("Failed to read OpenAPI file: {}", file, e);
              }
            });

    if (openapiFiles.isEmpty()) {
      throw new IOException("No OpenAPI specifications found in: " + sourceDir);
    }

    log.debug("Found {} OpenAPI file(s) to process", openapiFiles.size());

    // Load instructions.md if available
    String instructionsContent = "";
    Path instructionsPath = handbookPath.resolve("Agent.md");
    if (Files.exists(instructionsPath)) {
      try {
        instructionsContent = Files.readString(instructionsPath);
      } catch (Exception e) {
        log.debug("Could not load Agent.md, using empty content", e);
      }
    }

    // Prepare prompt context
    Map<String, Object> context = new HashMap<>();
    context.put("instructions_content", instructionsContent);
    context.put("openapi_files", openapiFiles);

    // Load and render lexicon extraction prompt
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("lexicon-extraction");
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("lexicon-extraction", context);

    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render lexicon extraction prompt template: {}", e.getMessage(), e);
      throw new ExecutionException(
          "Failed to render lexicon extraction prompt template: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from lexicon extraction prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    log.debug("Calling LLM for lexicon extraction");
    
    // Create telemetry sink to capture token usage and set phase for logging
    final long[] tokenCounts = new long[3]; // [promptTokens, completionTokens, totalTokens]
    final Map<String, Object> sinkAttributes = new HashMap<>();
    sinkAttributes.put("phase", "lexifier"); // Set phase so LLM client detects it correctly
    sinkAttributes.put("cacheHit", false); // Lexicon extraction is never cached
    
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
    
    // Get temperature from template if specified (lexicon extraction uses default)
    // Note: lexicon extraction doesn't use a template, so temperature is null (uses default)
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
      // LLM client will automatically log input messages and inference complete
      // with phase detected from telemetry sink attributes
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, null);
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
      throw new ExecutionException("No JSON content found in LLM response");
    }

    // Parse JSON response
    try {
      PromptLexicon lexicon = objectMapper.readValue(jsonStr, PromptLexicon.class);
      
      // Normalize all names: convert dots to underscores (fields, entities, actions)
      // This ensures consistency with snake_case convention
      if (lexicon.getFields() != null) {
        List<String> normalizedFields = new ArrayList<>();
        for (String field : lexicon.getFields()) {
          if (field != null && field.contains(".")) {
            String normalized = field.replace(".", "_");
            log.debug("Normalized field name: '{}' → '{}'", field, normalized);
            normalizedFields.add(normalized);
          } else {
            normalizedFields.add(field);
          }
        }
        lexicon.setFields(normalizedFields);
      }
      
      if (lexicon.getEntities() != null) {
        List<String> normalizedEntities = new ArrayList<>();
        for (String entity : lexicon.getEntities()) {
          if (entity != null && entity.contains(".")) {
            String normalized = entity.replace(".", "_");
            log.debug("Normalized entity name: '{}' → '{}'", entity, normalized);
            normalizedEntities.add(normalized);
          } else {
            normalizedEntities.add(entity);
          }
        }
        lexicon.setEntities(normalizedEntities);
      }
      
      if (lexicon.getActions() != null) {
        List<String> normalizedActions = new ArrayList<>();
        for (String action : lexicon.getActions()) {
          if (action != null && action.contains(".")) {
            String normalized = action.replace(".", "_");
            log.debug("Normalized action name: '{}' → '{}'", action, normalized);
            normalizedActions.add(normalized);
          } else {
            normalizedActions.add(action);
          }
        }
        lexicon.setActions(normalizedActions);
      }
      
      log.info(
          "Successfully extracted lexicon: {} actions, {} entities, {} fields",
          lexicon.getActions().size(),
          lexicon.getEntities().size(),
          lexicon.getFields().size());
      return lexicon;
    } catch (Exception e) {
      log.error("Failed to parse lexicon from LLM response", e);
      log.debug("Extracted JSON was: {}", jsonStr);
      log.debug("Full LLM response was: {}", response);
      throw new ExecutionException(
          "Failed to parse lexicon from LLM response: " + e.getMessage(), e);
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
   * Save lexicon to YAML file.
   *
   * @param lexicon the lexicon to save
   * @param outputPath path to output YAML file
   * @throws IOException if file operations fail
   */
  public void saveLexicon(PromptLexicon lexicon, Path outputPath) throws IOException {
    log.info("Saving lexicon to: {}", outputPath);
    Files.createDirectories(outputPath.getParent());
    // Use YAML mapper instead of JSON mapper
    JacksonUtility.getYamlMapper().writeValue(outputPath.toFile(), lexicon);
    log.info("Lexicon saved successfully");
  }

  /**
   * Load lexicon from YAML file.
   *
   * @param lexiconPath path to lexicon YAML file
   * @return loaded lexicon, or null if file doesn't exist
   * @throws IOException if file operations fail
   */
  public PromptLexicon loadLexicon(Path lexiconPath) throws IOException {
    if (!Files.exists(lexiconPath)) {
      log.debug("Lexicon file does not exist: {}", lexiconPath);
      return null;
    }

    log.debug("Loading lexicon from: {}", lexiconPath);
    try {
      PromptLexicon lexicon =
          JacksonUtility.getYamlMapper().readValue(lexiconPath.toFile(), PromptLexicon.class);

      log.info(
          "Successfully loaded lexicon: {} actions, {} entities, {} fields",
          lexicon.getActions() != null ? lexicon.getActions().size() : 0,
          lexicon.getEntities() != null ? lexicon.getEntities().size() : 0,
          lexicon.getFields() != null ? lexicon.getFields().size() : 0);

      if (lexicon.getActions() == null || lexicon.getActions().isEmpty()) {
        log.error("WARNING: Lexicon loaded but actions list is null or empty!");
      }

      return lexicon;
    } catch (Exception e) {
      log.error("Failed to load lexicon from: {}", lexiconPath, e);
      throw new IOException("Failed to load lexicon: " + e.getMessage(), e);
    }
  }
}
