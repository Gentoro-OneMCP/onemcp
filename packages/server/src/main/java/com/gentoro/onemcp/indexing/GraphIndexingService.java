package com.gentoro.onemcp.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.context.KnowledgeDocument;
import com.gentoro.onemcp.context.Operation;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.indexing.driver.GraphIndexingDriver;
import com.gentoro.onemcp.indexing.driver.arangodb.ArangoIndexingDriver;
import com.gentoro.onemcp.indexing.graph.*;
import com.gentoro.onemcp.indexing.graph.nodes.DocumentationNode;
import com.gentoro.onemcp.indexing.graph.nodes.EntityNode;
import com.gentoro.onemcp.indexing.graph.nodes.ExampleNode;
import com.gentoro.onemcp.indexing.graph.nodes.FieldNode;
import com.gentoro.onemcp.indexing.graph.nodes.OperationNode;
import com.gentoro.onemcp.indexing.logging.GraphIndexingLogger;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.tags.Tag;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

/**
 * Service for building a graph representation of the knowledge base in ArangoDB.
 *
 * <p>This service orchestrates the extraction and indexing of:
 *
 * <ul>
 *   <li>Entities from OpenAPI tags
 *   <li>Operations with signatures, examples, and documentation
 *   <li>Examples from OpenAPI specifications
 *   <li>Relationships between all these elements
 * </ul>
 *
 * <p>The resulting graph enables semantic querying and retrieval based on relationships.
 */
public class GraphIndexingService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(GraphIndexingService.class);

  private final OneMcp oneMcp;
  private final GraphIndexingDriver graphDriver;
  private final String driverName;
  private final String handbookName;
  private final GraphIndexingLogger indexingLogger;

  /**
   * Create a new graph indexing service.
   *
   * @param oneMcp the OneMcp application context
   */
  public GraphIndexingService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    this.handbookName = oneMcp.knowledgeBase().getHandbookName();
    this.driverName =
        oneMcp
            .configuration()
            .getString("graph.indexing.driver", "arangodb")
            .toLowerCase(java.util.Locale.ROOT);
    this.graphDriver = createDriver(driverName);
    this.indexingLogger = new GraphIndexingLogger(handbookName);
  }

  /**
   * Create a graph indexing driver instance based on the driver name.
   *
   * @param name the driver name (e.g., "arangodb")
   * @return the driver instance, or null if the driver is not supported
   */
  private GraphIndexingDriver createDriver(String name) {
    return switch (name) {
      case "arangodb" -> new ArangoIndexingDriver(oneMcp, handbookName);
      default -> {
        log.warn("Graph indexing driver '{}' not supported yet, initialization skipped", name);
        yield null;
      }
    };
  }

  /**
   * Index the entire knowledge base into the graph database.
   *
   * <p>This is the main entry point that coordinates all indexing operations.
   */
  public void indexKnowledgeBase() {
    log.info("Starting knowledge base graph indexing (driver: {})", driverName);

    if (graphDriver == null) {
      log.info("Graph indexing driver '{}' not implemented; skipping graph indexing", driverName);
      return;
    }

    try {
      // Initialize ArangoDB service
      graphDriver.initialize();

      if (!graphDriver.isInitialized()) {
        log.info("ArangoDB not enabled or not available, graph indexing skipped");
        return;
      }

      // Clear existing data to avoid duplicate key errors on re-indexing
      boolean clearData = shouldClearOnStartup();
      if (clearData) {
        log.info("Clearing existing graph data before re-indexing");
        graphDriver.clearAllData();
      }

      // Index services and their components using LLM-based extraction
      List<Service> services = oneMcp.knowledgeBase().services();
      log.info("Indexing {} services using LLM-based extraction", services.size());

      for (Service service : services) {
        indexServiceWithLLM(service);
      }

      // Ensure graph exists after indexing (in case it wasn't created during clear)
      graphDriver.ensureGraphExists();

      log.info("Knowledge base graph indexing completed successfully");
    } catch (Exception e) {
      log.error("Failed to index knowledge base graph", e);
      throw new com.gentoro.onemcp.exception.IoException("Graph indexing failed", e);
    }
  }

  /**
   * Determine if the graph data should be cleared on startup based on configuration.
   *
   * <p>Checks both global and driver-specific configuration keys, with fallback to legacy keys.
   *
   * @return true if data should be cleared, false otherwise
   */
  private boolean shouldClearOnStartup() {
    Configuration config = oneMcp.configuration();
    String globalKey = "graph.indexing.clearOnStartup";
    String driverKey = "graph.indexing." + driverName + ".clearOnStartup";

    if (config.containsKey(globalKey)) {
      return config.getBoolean(globalKey);
    }
    if (config.containsKey(driverKey)) {
      return config.getBoolean(driverKey);
    }

    // Legacy fallback for earlier configuration
    return config.getBoolean("arangodb.clearOnStartup", true);
  }

  /**
   * Index a single service using LLM-based extraction of entities and operations.
   *
   * @param service the service to index
   */
  private void indexServiceWithLLM(Service service) {
    log.debug("Indexing service with LLM: {}", service.getSlug());

    try {
      // Check if LLM client is available
      LlmClient llmClient = oneMcp.llmClient();
      if (llmClient == null) {
        log.warn(
            "LLM client not available, falling back to rule-based extraction for service: {}",
            service.getSlug());
        indexService(service);
        return;
      }

      // Load prompt template
      PromptTemplate promptTemplate = oneMcp.promptRepository().get("api-extraction");
      PromptTemplate.PromptSession session = promptTemplate.newSession();

      // Prepare context variables for the prompt
      Map<String, Object> context = preparePromptContext(service);

      // Enable the activation section with context
      // All sections in the template use the same activation ID "api-extraction"
      // Enabling it once will enable all sections with that ID
      session.enable("api-extraction", context);

      // Render messages and verify we have content to send
      List<LlmClient.Message> messages = session.renderMessages();
      if (messages.isEmpty()) {
        log.warn(
            "No messages rendered from prompt template, falling back to rule-based extraction");
        indexService(service);
        return;
      }

      log.debug(
          "Rendered {} messages from prompt template, calling LLM for service: {}",
          messages.size(),
          service.getSlug());

      // Log prompt to file
      indexingLogger.logLLMPrompt(service.getSlug(), messages);

      String llmResponse = llmClient.chat(messages, Collections.emptyList(), false, null);

      // Log response to file
      indexingLogger.logLLMResponse(service.getSlug(), llmResponse);

      // Parse LLM response and extract entities/operations
      GraphIndexingTypes.GraphExtractionResult result =
          parseLLMResponse(llmResponse, service.getSlug());

      // Index extracted entities
      for (EntityNode entity : result.entities()) {
        graphDriver.storeNode(entity);
      }

      // Index extracted fields
      for (FieldNode field : result.fields()) {
        graphDriver.storeNode(field);
      }

      // Index extracted operations (deduplicate by key to ensure shared operations)
      Set<String> seenOperationKeys = new HashSet<>();
      for (OperationNode operation : result.operations()) {
        if (!seenOperationKeys.contains(operation.getKey())) {
          graphDriver.storeNode(operation);
          seenOperationKeys.add(operation.getKey());
        } else {
          log.debug("Skipping duplicate operation: {}", operation.getKey());
        }
      }

      // Index extracted examples
      for (ExampleNode example : result.examples()) {
        graphDriver.storeNode(example);

        // Create edge from operation to example if operationKey is provided
        // Only create if it doesn't already exist in the relationships list
        if (example.getOperationKey() != null && !example.getOperationKey().isEmpty()) {
          // Check if this edge already exists in relationships (check for common example edge
          // types)
          boolean edgeExists =
              result.relationships().stream()
                  .anyMatch(
                      edge ->
                          edge.getFromKey().equals(example.getOperationKey())
                              && edge.getToKey().equals(example.getKey())
                              && (edge.getEdgeType().equals("HAS_EXAMPLE")
                                  || edge.getEdgeType().equals("DEMONSTRATES")
                                  || edge.getEdgeType().equals("ILLUSTRATES")));

          if (!edgeExists) {
            GraphEdge exampleEdge =
                new GraphEdge(
                    example.getOperationKey(),
                    example.getKey(),
                    "HAS_EXAMPLE",
                    new HashMap<>(),
                    "Operation has this example",
                    "strong");
            graphDriver.storeEdge(exampleEdge);
          }
        }
      }

      // Index extracted documentations
      for (DocumentationNode documentation : result.documentations()) {
        graphDriver.storeNode(documentation);
      }

      // Build a set of all valid node keys for validation
      Set<String> validNodeKeys = new HashSet<>();
      result.entities().forEach(e -> validNodeKeys.add(e.getKey()));
      result.fields().forEach(f -> validNodeKeys.add(f.getKey()));
      result.operations().forEach(o -> validNodeKeys.add(o.getKey()));
      result.examples().forEach(ex -> validNodeKeys.add(ex.getKey()));
      result.documentations().forEach(d -> validNodeKeys.add(d.getKey()));

      // Index relationships, filtering out those referencing non-existent nodes and deduplicating
      int validRelationships = 0;
      int skippedRelationships = 0;
      Set<String> seenEdges = new HashSet<>();

      for (GraphEdge edge : result.relationships()) {
        // Create a unique identifier for this edge
        String edgeId = edge.getFromKey() + "|" + edge.getEdgeType() + "|" + edge.getToKey();

        // Skip duplicate edges
        if (seenEdges.contains(edgeId)) {
          log.debug(
              "Skipping duplicate edge: {} -> {} (type: {})",
              edge.getFromKey(),
              edge.getToKey(),
              edge.getEdgeType());
          skippedRelationships++;
          continue;
        }

        // Validate that both source and target nodes exist
        if (!validNodeKeys.contains(edge.getFromKey())) {
          log.debug(
              "Skipping edge from non-existent node: {} -> {} (type: {})",
              edge.getFromKey(),
              edge.getToKey(),
              edge.getEdgeType());
          skippedRelationships++;
          continue;
        }
        if (!validNodeKeys.contains(edge.getToKey())) {
          log.debug(
              "Skipping edge to non-existent node: {} -> {} (type: {})",
              edge.getFromKey(),
              edge.getToKey(),
              edge.getEdgeType());
          skippedRelationships++;
          continue;
        }

        graphDriver.storeEdge(edge);
        seenEdges.add(edgeId);
        validRelationships++;
      }

      if (skippedRelationships > 0) {
        log.warn(
            "Skipped {} invalid/duplicate relationships for service: {}",
            skippedRelationships,
            service.getSlug());
      }

      // Log complete graph structure
      indexingLogger.logCompleteGraph(service.getSlug(), result);

      log.info(
          "Indexed service {} with {} entities, {} fields, {} operations, {} examples, {} documentations, {} valid relationships ({} skipped) (LLM-based)",
          service.getSlug(),
          result.entities().size(),
          result.fields().size(),
          result.operations().size(),
          result.examples().size(),
          result.documentations().size(),
          validRelationships,
          skippedRelationships);
    } catch (Exception e) {
      log.error(
          "Failed to index service with LLM: {}, falling back to rule-based extraction",
          service.getSlug(),
          e);
      // Fallback to rule-based extraction
      indexService(service);
    }
  }

  /**
   * Prepare context variables for the LLM prompt.
   *
   * @param service the service to prepare context for
   * @return map of context variables
   */
  private Map<String, Object> preparePromptContext(Service service) {
    Map<String, Object> context = new HashMap<>();

    try {
      // Load instructions.md content - ensure it's never null
      String instructionsContent = "";
      try {
        List<KnowledgeDocument> instructions =
            oneMcp.knowledgeBase().findByUriPrefix("kb:///instructions.md");
        if (!instructions.isEmpty()) {
          String content = instructions.get(0).content();
          instructionsContent = (content != null) ? content : "";
        }
      } catch (Exception e) {
        log.debug("Could not load instructions.md, using empty content", e);
      }
      context.put("instructions_content", instructionsContent);

      // Load OpenAPI files - always initialize as empty list
      List<Map<String, String>> openapiFiles = new ArrayList<>();
      Path handbookPath = oneMcp.knowledgeBase().handbookPath();
      Path openapiPath = handbookPath.resolve("openapi");
      if (Files.exists(openapiPath)) {
        try {
          Files.walk(openapiPath)
              .filter(Files::isRegularFile)
              .filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
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
        } catch (Exception e) {
          log.warn("Failed to walk OpenAPI directory", e);
        }
      }
      context.put("openapi_files", openapiFiles);

      // Load documentation files from docs/ folder - always initialize as empty list
      List<Map<String, String>> docsFiles = new ArrayList<>();
      Path docsPath = handbookPath.resolve("docs");
      if (Files.exists(docsPath)) {
        try {
          Files.walk(docsPath)
              .filter(Files::isRegularFile)
              .filter(
                  p -> {
                    String fileName = p.getFileName().toString().toLowerCase();
                    return fileName.endsWith(".md")
                        || fileName.endsWith(".markdown")
                        || fileName.endsWith(".txt")
                        || fileName.endsWith(".mdx");
                  })
              .forEach(
                  file -> {
                    try {
                      Map<String, String> fileInfo = new HashMap<>();
                      fileInfo.put("name", file.getFileName().toString());
                      fileInfo.put("content", Files.readString(file));
                      docsFiles.add(fileInfo);
                    } catch (Exception e) {
                      log.warn("Failed to read documentation file: {}", file, e);
                    }
                  });
        } catch (Exception e) {
          log.warn("Failed to walk docs directory", e);
        }
      }
      context.put("docs_files", docsFiles);

    } catch (Exception e) {
      log.warn("Error preparing prompt context", e);
    }

    return context;
  }

  /**
   * Parse LLM JSON response and extract entities, operations, and relationships.
   *
   * @param llmResponse the LLM response string (should be JSON)
   * @param serviceSlug the service slug
   * @return extraction result with entities, operations, and relationships
   */
  private GraphIndexingTypes.GraphExtractionResult parseLLMResponse(
      String llmResponse, String serviceSlug) {
    List<EntityNode> entities = new ArrayList<>();
    List<FieldNode> fields = new ArrayList<>();
    List<OperationNode> operations = new ArrayList<>();
    List<ExampleNode> examples = new ArrayList<>();
    List<DocumentationNode> documentations = new ArrayList<>();
    List<GraphEdge> relationships = new ArrayList<>();

    try {
      // Extract JSON from response (may be wrapped in markdown code blocks)
      String jsonStr = extractJsonFromResponse(llmResponse);

      if (jsonStr == null || jsonStr.trim().isEmpty()) {
        log.warn("No JSON content found in LLM response for service: {}", serviceSlug);
        indexingLogger.logLLMParseError(
            serviceSlug, llmResponse, new IllegalArgumentException("No JSON content found"));
        return new GraphIndexingTypes.GraphExtractionResult(
            entities, fields, operations, examples, documentations, relationships);
      }

      // Try to fix common JSON issues (truncated strings, unclosed structures)
      String fixedJson = fixTruncatedJson(jsonStr);

      // Parse JSON
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      @SuppressWarnings("unchecked")
      Map<String, Object> result = mapper.readValue(fixedJson, Map.class);

      // Extract entities
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> entitiesList = (List<Map<String, Object>>) result.get("entities");
      if (entitiesList != null) {
        for (Map<String, Object> entityData : entitiesList) {
          String key = (String) entityData.get("key");
          String name = (String) entityData.get("name");
          String description = (String) entityData.getOrDefault("description", "");
          @SuppressWarnings("unchecked")
          List<String> associatedOps =
              (List<String>) entityData.getOrDefault("associatedOperations", new ArrayList<>());
          String source = (String) entityData.getOrDefault("source", null);
          @SuppressWarnings("unchecked")
          List<String> attributes = (List<String>) entityData.getOrDefault("attributes", null);
          String domain = (String) entityData.getOrDefault("domain", null);

          EntityNode entity =
              new EntityNode(
                  key != null ? key : "entity|" + sanitizeKey(name),
                  name,
                  description,
                  serviceSlug,
                  associatedOps,
                  source,
                  attributes,
                  domain);
          entities.add(entity);
        }
      }

      // Extract fields
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> fieldsList = (List<Map<String, Object>>) result.get("fields");
      if (fieldsList != null) {
        for (Map<String, Object> fieldData : fieldsList) {
          String key = (String) fieldData.get("key");
          String name = (String) fieldData.get("name");
          String description = (String) fieldData.getOrDefault("description", "");
          String fieldType = (String) fieldData.getOrDefault("fieldType", "string");
          String entityKey = (String) fieldData.get("entityKey");
          String source = (String) fieldData.getOrDefault("source", null);

          // Generate key if not provided
          if (key == null && entityKey != null && name != null) {
            String entityName = entityKey.replace("entity|", "");
            key = "field|" + sanitizeKey(entityName + "_" + name);
          } else if (key == null) {
            log.warn("Field missing key, name, or entityKey, skipping");
            continue;
          }

          FieldNode field =
              new FieldNode(key, name, description, fieldType, entityKey, serviceSlug, source);
          fields.add(field);
        }
      }

      // Extract operations
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> operationsList =
          (List<Map<String, Object>>) result.get("operations");
      if (operationsList != null) {
        for (Map<String, Object> opData : operationsList) {
          String key = (String) opData.get("key");
          String operationId = (String) opData.get("operationId");
          String method = (String) opData.get("method");
          String path = (String) opData.get("path");
          String summary = (String) opData.getOrDefault("summary", "");
          String description = (String) opData.getOrDefault("description", summary);
          @SuppressWarnings("unchecked")
          List<String> tags = (List<String>) opData.getOrDefault("tags", new ArrayList<>());
          String signature =
              (String) opData.getOrDefault("signature", method + " " + path + " - " + summary);
          @SuppressWarnings("unchecked")
          List<String> exampleKeys =
              (List<String>) opData.getOrDefault("exampleKeys", new ArrayList<>());
          String documentationUri = (String) opData.getOrDefault("documentationUri", "");
          Object requestSchemaObj = opData.getOrDefault("requestSchema", null);
          String requestSchema =
              requestSchemaObj != null ? convertToString(requestSchemaObj) : null;
          Object responseSchemaObj = opData.getOrDefault("responseSchema", null);
          String responseSchema =
              responseSchemaObj != null ? convertToString(responseSchemaObj) : null;
          @SuppressWarnings("unchecked")
          List<String> operationExamples = (List<String>) opData.getOrDefault("examples", null);
          String category = (String) opData.getOrDefault("category", null);
          String primaryEntity = (String) opData.getOrDefault("primaryEntity", null);

          // Validate required fields
          if (operationId == null || operationId.trim().isEmpty()) {
            log.warn("Operation missing operationId, skipping");
            continue;
          }

          // Log category extraction for debugging
          if (category != null) {
            log.trace("Extracted category '{}' for operation: {}", category, operationId);
          } else {
            log.debug("No category found for operation: {}", operationId);
          }

          // Log primaryEntity extraction for debugging
          if (primaryEntity != null) {
            log.trace("Extracted primaryEntity '{}' for operation: {}", primaryEntity, operationId);
          } else {
            log.debug("No primaryEntity found for operation: {}", operationId);
          }

          OperationNode operation =
              new OperationNode(
                  key != null ? key : "op|" + sanitizeKey(operationId),
                  operationId,
                  method,
                  path,
                  summary,
                  description,
                  serviceSlug,
                  tags,
                  signature,
                  exampleKeys,
                  documentationUri,
                  requestSchema,
                  responseSchema,
                  operationExamples,
                  category,
                  primaryEntity);
          operations.add(operation);
        }
      }

      // Extract examples
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> examplesList = (List<Map<String, Object>>) result.get("examples");
      if (examplesList != null) {
        for (Map<String, Object> exData : examplesList) {
          String key = (String) exData.get("key");
          String name = (String) exData.get("name");
          String summary = (String) exData.getOrDefault("summary", "");
          String description = (String) exData.getOrDefault("description", summary);

          // Handle requestBody and responseBody which might be objects or strings
          Object requestBodyObj = exData.getOrDefault("requestBody", "");
          String requestBody = convertToString(requestBodyObj);

          Object responseBodyObj = exData.getOrDefault("responseBody", "");
          String responseBody = convertToString(responseBodyObj);

          // Handle responseStatus which might be a number or string
          Object responseStatusObj = exData.getOrDefault("responseStatus", "200");
          String responseStatus = responseStatusObj != null ? responseStatusObj.toString() : "200";

          String operationKey = (String) exData.get("operationKey");

          // Generate key if not provided
          if (key == null && operationKey != null && name != null) {
            key = "example|" + sanitizeKey(operationKey.replace("op|", "") + "_" + name);
          } else if (key == null) {
            log.warn("Example missing key, name, or operationKey, skipping");
            continue;
          }

          ExampleNode example =
              new ExampleNode(
                  key,
                  name != null ? name : "",
                  summary,
                  description,
                  requestBody,
                  responseBody,
                  responseStatus,
                  operationKey,
                  serviceSlug);
          examples.add(example);
        }
      }

      // Extract documentations
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> documentationsList =
          (List<Map<String, Object>>) result.get("documentations");
      if (documentationsList != null) {
        for (Map<String, Object> docData : documentationsList) {
          String key = (String) docData.get("key");
          String title = (String) docData.getOrDefault("title", "");
          String content = (String) docData.getOrDefault("content", "");
          String docType = (String) docData.getOrDefault("docType", "concept");
          String sourceFile = (String) docData.getOrDefault("sourceFile", null);
          @SuppressWarnings("unchecked")
          List<String> relatedKeys =
              (List<String>) docData.getOrDefault("relatedKeys", new ArrayList<>());
          @SuppressWarnings("unchecked")
          Map<String, String> metadata =
              (Map<String, String>) docData.getOrDefault("metadata", new HashMap<>());

          // Generate key if not provided
          if (key == null && title != null) {
            key = "doc|" + sanitizeKey(title);
          } else if (key == null) {
            log.warn("Documentation missing key and title, skipping");
            continue;
          }

          // Validate required fields
          if (content == null || content.trim().isEmpty()) {
            log.debug("Documentation {} has empty content, skipping", key);
            continue;
          }

          DocumentationNode documentation =
              new DocumentationNode(
                  key, title, content, docType, sourceFile, relatedKeys, serviceSlug, metadata);
          documentations.add(documentation);
        }
      }

      // Extract relationships
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> relationshipsList =
          (List<Map<String, Object>>) result.get("relationships");
      if (relationshipsList != null) {
        for (Map<String, Object> relData : relationshipsList) {
          String fromKey = (String) relData.get("fromKey");
          String toKey = (String) relData.get("toKey");
          String edgeTypeStr = (String) relData.get("edgeType");
          String description = (String) relData.getOrDefault("description", null);
          String strength = (String) relData.getOrDefault("strength", null);

          // Validate that edgeType is provided
          if (edgeTypeStr == null || edgeTypeStr.trim().isEmpty()) {
            log.warn("Missing edge type for relationship {} -> {}, skipping", fromKey, toKey);
            continue;
          }

          GraphEdge edge =
              new GraphEdge(fromKey, toKey, edgeTypeStr, new HashMap<>(), description, strength);
          relationships.add(edge);
        }
      }

    } catch (Exception e) {
      log.error("Failed to parse LLM response for service: {}, using empty result", serviceSlug, e);
      indexingLogger.logLLMParseError(serviceSlug, llmResponse, e);
    }

    return new GraphIndexingTypes.GraphExtractionResult(
        entities, fields, operations, examples, documentations, relationships);
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
   * Attempt to fix truncated JSON by closing unclosed strings and structures.
   *
   * @param jsonStr the JSON string to fix
   * @return fixed JSON string
   */
  private String fixTruncatedJson(String jsonStr) {
    if (jsonStr == null) {
      return "{}";
    }

    StringBuilder fixed = new StringBuilder(jsonStr.length() + 200);
    boolean inString = false;
    boolean escaped = false;
    int braceDepth = 0;
    int bracketDepth = 0;
    int lastValidPos = -1;

    for (int i = 0; i < jsonStr.length(); i++) {
      char c = jsonStr.charAt(i);

      if (escaped) {
        fixed.append(c);
        escaped = false;
        continue;
      }

      if (c == '\\') {
        escaped = true;
        fixed.append(c);
        continue;
      }

      if (c == '"') {
        inString = !inString;
        fixed.append(c);
        if (!inString) {
          lastValidPos = fixed.length();
        }
        continue;
      }

      if (!inString) {
        if (c == '{') {
          braceDepth++;
          fixed.append(c);
          lastValidPos = fixed.length();
        } else if (c == '}') {
          braceDepth--;
          fixed.append(c);
          lastValidPos = fixed.length();
        } else if (c == '[') {
          bracketDepth++;
          fixed.append(c);
          lastValidPos = fixed.length();
        } else if (c == ']') {
          bracketDepth--;
          fixed.append(c);
          lastValidPos = fixed.length();
        } else {
          fixed.append(c);
          if (Character.isWhitespace(c) || c == ',' || c == ':') {
            lastValidPos = fixed.length();
          }
        }
      } else {
        // Inside a string - just append
        fixed.append(c);
      }
    }

    // If we're still in a string, close it
    if (inString) {
      fixed.append('"');
      log.warn("Fixed unclosed string in JSON by adding closing quote");
    }

    // Close any unclosed brackets/braces
    while (bracketDepth > 0) {
      fixed.append(']');
      bracketDepth--;
      log.warn("Fixed unclosed array bracket in JSON");
    }

    while (braceDepth > 0) {
      fixed.append('}');
      braceDepth--;
      log.warn("Fixed unclosed object brace in JSON");
    }

    // If JSON appears to be truncated mid-value, try to close the last incomplete object/array
    String result = fixed.toString();

    // If the last character before our fixes was a comma, remove it
    if (lastValidPos > 0 && lastValidPos < result.length()) {
      String beforeFix = result.substring(0, lastValidPos);
      String afterFix = result.substring(lastValidPos);

      // If we added closing braces and there's a trailing comma before them, clean it up
      if (afterFix.startsWith("}") || afterFix.startsWith("]")) {
        // Remove trailing comma if present
        beforeFix = beforeFix.replaceAll(",\\s*$", "");
        result = beforeFix + afterFix;
      }
    }

    return result;
  }

  /**
   * Convert an object to a string, handling both String and complex objects (like Map). If the
   * object is a Map or other complex type, serialize it to JSON string.
   *
   * @param obj the object to convert
   * @return string representation
   */
  private String convertToString(Object obj) {
    if (obj == null) {
      return "";
    }
    if (obj instanceof String) {
      return (String) obj;
    }
    // For complex objects (Map, List, etc.), serialize to JSON
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      return mapper.writeValueAsString(obj);
    } catch (Exception e) {
      log.warn("Failed to serialize object to JSON string, using toString()", e);
      return obj.toString();
    }
  }

  /**
   * Index a single service using rule-based extraction (fallback method).
   *
   * @param service the service to index
   */
  private void indexService(Service service) {
    log.debug("Indexing service (rule-based): {}", service.getSlug());

    try {
      // Load OpenAPI definition
      OpenAPI openAPI = service.definition(oneMcp.knowledgeBase().handbookPath());

      // Extract and index entities (tags)
      List<EntityNode> entities = extractEntities(service, openAPI);
      for (EntityNode entity : entities) {
        graphDriver.storeNode(entity);
      }

      // Extract and index operations
      List<OperationNode> operationNodes = extractOperations(service, openAPI);
      for (OperationNode opNode : operationNodes) {
        graphDriver.storeNode(opNode);

        // Create edges from entities to operations
        for (String tag : opNode.getTags()) {
          String entityKey = "entity|" + sanitizeKey(tag);
          graphDriver.storeEdge(new GraphEdge(entityKey, opNode.getKey(), "HAS_OPERATION"));
        }
      }

      log.debug(
          "Indexed service {} with {} entities, {} operations (rule-based)",
          service.getSlug(),
          entities.size(),
          operationNodes.size());
    } catch (Exception e) {
      log.error("Failed to index service: {}", service.getSlug(), e);
    }
  }

  /**
   * Extract entity nodes from OpenAPI tags.
   *
   * @param service the service
   * @param openAPI the OpenAPI specification
   * @return list of entity nodes
   */
  private List<EntityNode> extractEntities(Service service, OpenAPI openAPI) {
    List<EntityNode> entities = new ArrayList<>();

    if (openAPI.getTags() == null || openAPI.getTags().isEmpty()) {
      log.debug("No tags found in service: {}", service.getSlug());
      return entities;
    }

    for (Tag tag : openAPI.getTags()) {
      String key = "entity|" + sanitizeKey(tag.getName());
      String description = tag.getDescription() != null ? tag.getDescription() : "";

      // Find operations associated with this tag
      List<String> associatedOps =
          service.getOperations().stream()
              .filter(op -> op.getOperation() != null)
              .map(op -> "op|" + sanitizeKey(op.getOperation()))
              .collect(Collectors.toList());

      EntityNode entity =
          new EntityNode(key, tag.getName(), description, service.getSlug(), associatedOps);
      entities.add(entity);
    }

    return entities;
  }

  /**
   * Extract operation nodes from service operations.
   *
   * @param service the service
   * @param openAPI the OpenAPI specification
   * @return list of operation nodes
   */
  private List<OperationNode> extractOperations(Service service, OpenAPI openAPI) {
    List<OperationNode> operationNodes = new ArrayList<>();

    for (Operation operation : service.getOperations()) {
      String key = "op|" + sanitizeKey(operation.getOperation());
      String signature = buildOperationSignature(operation);

      // Extract tags from operation
      List<String> tags = new ArrayList<>();
      // Note: tags would need to be extracted from OpenAPI paths, simplified for now
      if (openAPI.getTags() != null) {
        tags = openAPI.getTags().stream().map(Tag::getName).collect(Collectors.toList());
      }

      // Find example keys for this operation
      List<String> exampleKeys = new ArrayList<>();
      // Examples will be linked later

      OperationNode opNode =
          new OperationNode(
              key,
              operation.getOperation(),
              operation.getMethod(),
              operation.getPath(),
              operation.getSummary(),
              operation.getSummary(), // Using summary as description
              service.getSlug(),
              tags,
              signature,
              exampleKeys,
              operation.getDocumentationUri());

      operationNodes.add(opNode);
    }

    return operationNodes;
  }

  /**
   * Build a signature string for an operation.
   *
   * @param operation the operation
   * @return signature string
   */
  private String buildOperationSignature(Operation operation) {
    return String.format(
        "%s %s - %s",
        operation.getMethod().toUpperCase(), operation.getPath(), operation.getSummary());
  }

  /**
   * Sanitize a string to be used as a key in ArangoDB.
   *
   * @param input the input string
   * @return sanitized string
   */
  private String sanitizeKey(String input) {
    if (input == null) return "";
    return input.replaceAll("[^a-zA-Z0-9_\\-|]", "_").toLowerCase();
  }

  /** Shutdown the indexing service and release resources. */
  public void shutdown() {
    if (graphDriver != null) {
      graphDriver.shutdown();
    }
  }
}
