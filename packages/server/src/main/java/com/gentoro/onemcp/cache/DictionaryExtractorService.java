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
 * Service for extracting canonical dictionary from OpenAPI specifications.
 *
 * <p>Uses LLM to extract actions, entities, fields, operators, and aggregates from API specs
 * according to the Prompt Schema Specification.
 */
public class DictionaryExtractorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(DictionaryExtractorService.class);

  private final OneMcp oneMcp;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public DictionaryExtractorService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Extract dictionary from OpenAPI specifications in the handbook.
   *
   * @param handbookPath path to the handbook directory
   * @return extracted dictionary
   * @throws IOException if file operations fail
   * @throws ExecutionException if LLM extraction fails
   */
  public PromptDictionary extractDictionary(Path handbookPath)
      throws IOException, ExecutionException {
    log.info("Extracting dictionary from handbook: {}", handbookPath);

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
    Path instructionsPath = handbookPath.resolve("Agent.yaml");
    if (Files.exists(instructionsPath)) {
      try {
        instructionsContent = Files.readString(instructionsPath);
      } catch (Exception e) {
        log.debug("Could not load Agent.yaml, using empty content", e);
      }
    }

    // Prepare prompt context
    Map<String, Object> context = new HashMap<>();
    context.put("instructions_content", instructionsContent);
    context.put("openapi_files", openapiFiles);

    // Load and render dictionary extraction prompt
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("dictionary-extraction");
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("dictionary-extraction", context);

    List<LlmClient.Message> messages = session.renderMessages();
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from dictionary extraction prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    log.debug("Calling LLM for dictionary extraction");
    // Pass empty list instead of null for tools parameter
    String response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null);

    // Extract JSON from response (may be wrapped in markdown code blocks)
    String jsonStr = extractJsonFromResponse(response);
    if (jsonStr == null || jsonStr.trim().isEmpty()) {
      log.error("No JSON content found in LLM response");
      log.debug("LLM response was: {}", response);
      throw new ExecutionException("No JSON content found in LLM response");
    }

    // Parse JSON response
    try {
      PromptDictionary dictionary = objectMapper.readValue(jsonStr, PromptDictionary.class);
      log.info(
          "Successfully extracted dictionary: {} actions, {} entities, {} fields",
          dictionary.getActions().size(),
          dictionary.getEntities().size(),
          dictionary.getFields().size());
      return dictionary;
    } catch (Exception e) {
      log.error("Failed to parse dictionary from LLM response", e);
      log.debug("Extracted JSON was: {}", jsonStr);
      log.debug("Full LLM response was: {}", response);
      throw new ExecutionException(
          "Failed to parse dictionary from LLM response: " + e.getMessage(), e);
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
   * Save dictionary to YAML file.
   *
   * @param dictionary the dictionary to save
   * @param outputPath path to output YAML file
   * @throws IOException if file operations fail
   */
  public void saveDictionary(PromptDictionary dictionary, Path outputPath) throws IOException {
    log.info("Saving dictionary to: {}", outputPath);
    Files.createDirectories(outputPath.getParent());
    // Use YAML mapper instead of JSON mapper
    JacksonUtility.getYamlMapper().writeValue(outputPath.toFile(), dictionary);
    log.info("Dictionary saved successfully");
  }

  /**
   * Load dictionary from YAML file.
   *
   * @param dictionaryPath path to dictionary YAML file
   * @return loaded dictionary, or null if file doesn't exist
   * @throws IOException if file operations fail
   */
  public PromptDictionary loadDictionary(Path dictionaryPath) throws IOException {
    if (!Files.exists(dictionaryPath)) {
      log.debug("Dictionary file does not exist: {}", dictionaryPath);
      return null;
    }

    log.debug("Loading dictionary from: {}", dictionaryPath);
    try {
      PromptDictionary dictionary =
          JacksonUtility.getYamlMapper().readValue(dictionaryPath.toFile(), PromptDictionary.class);

      log.info(
          "Successfully loaded dictionary: {} actions, {} entities, {} fields",
          dictionary.getActions() != null ? dictionary.getActions().size() : 0,
          dictionary.getEntities() != null ? dictionary.getEntities().size() : 0,
          dictionary.getFields() != null ? dictionary.getFields().size() : 0);

      if (dictionary.getActions() == null || dictionary.getActions().isEmpty()) {
        log.error("WARNING: Dictionary loaded but actions list is null or empty!");
      }

      return dictionary;
    } catch (Exception e) {
      log.error("Failed to load dictionary from: {}", dictionaryPath, e);
      throw new IOException("Failed to load dictionary: " + e.getMessage(), e);
    }
  }
}
