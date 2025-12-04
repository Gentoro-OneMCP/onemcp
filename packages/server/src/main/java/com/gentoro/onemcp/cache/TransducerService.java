package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.*;

/**
 * Transducer service (cache_spec v12.0).
 *
 * <p>Converts OpenAPI specifications into:
 * 1. Schema - tables with columns
 * 2. Endpoint Index - mapping from tables → API endpoints
 *
 * <p>Implements a single-pass pipeline with 4 phases:
 * <ol>
 *   <li>Phase 1: Endpoint Description Catalog - extract all endpoint descriptions
 *   <li>Phase 2: Schema Generation (LLM) - identify tables and map endpoints
 *   <li>Phase 3: Column Extraction - extract and assign fields to tables
 *   <li>Phase 4: Lookup Methods - build get_columns() and get_endpoints()
 * </ol>
 *
 * <p>Outputs:
 * <ul>
 *   <li>conceptual_schema.yaml - tables and columns
 *   <li>cache/endpoint_info/<table_name>.json - endpoint indexes per table
 *   <li>cache/endpoint_info/endpoints_descriptions.json - endpoint catalog
 * </ul>
 */
public class TransducerService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(TransducerService.class);

  private final OneMcp oneMcp;
  private final ObjectMapper objectMapper = JacksonUtility.getJsonMapper();
  private final Path cacheDir;
  private final Path endpointInfoDir;

  public TransducerService(OneMcp oneMcp, Path cacheDir) {
    this.oneMcp = oneMcp;
    this.cacheDir = cacheDir;
    this.endpointInfoDir = cacheDir.resolve("endpoint_info");
  }

  /**
   * Extract schema and endpoint indexes from OpenAPI specifications.
   *
   * @param handbookPath path to the handbook directory
   * @param cacheIndexDir directory to store cache/endpoint_info/<table_name>.json files (unused, kept for compatibility)
   * @return extracted schema
   * @throws IOException if file operations fail
   * @throws ExecutionException if LLM extraction fails
   */
  public ConceptualSchema extractConceptualSchema(Path handbookPath, Path cacheIndexDir)
      throws IOException, ExecutionException {
    log.info("Starting transducer extraction from handbook: {}", handbookPath);

    // Ensure directories exist
    Files.createDirectories(endpointInfoDir);

    // Phase 1: Endpoint Description Catalog
    log.info("Phase 1: Extracting endpoint descriptions");
    OpenApiExtractionResult extractionResult = extractAllEndpoints(handbookPath);
    List<EndpointInfo> allEndpoints = extractionResult.endpoints;
    io.swagger.v3.oas.models.OpenAPI openAPI = extractionResult.openAPI;
    log.info("Found {} total endpoints", allEndpoints.size());
    
    Path endpointsFile = endpointInfoDir.resolve("endpoints_descriptions.json");
    saveEndpointDescriptions(allEndpoints, endpointsFile);
    log.info("Saved endpoint descriptions to: {}", endpointsFile);
    
    // Save OpenAPI description for easy access by planner
    saveOpenApiDescription(openAPI);

    // Phase 2: Schema Generation (LLM)
    log.info("Phase 2: Generating schema with LLM");
    SchemaResult schemaResult = generateSchemaWithLLM(allEndpoints, openAPI, handbookPath);
    
    // Write tables-only schema (or with LLM-generated columns if available)
    ConceptualSchema tablesOnlySchema = new ConceptualSchema();
    Map<String, ConceptualSchema.TableDefinition> tables = new HashMap<>();
    for (String table : schemaResult.tables.keySet()) {
      // Check if LLM generated columns for this table
      List<String> llmColumns = schemaResult.getColumns(table);
      if (llmColumns != null && !llmColumns.isEmpty()) {
        log.info("Using LLM-generated columns for table '{}': {}", table, llmColumns);
        tables.put(table, new ConceptualSchema.TableDefinition(llmColumns));
      } else {
        tables.put(table, new ConceptualSchema.TableDefinition(new ArrayList<>()));
      }
    }
    tablesOnlySchema.setTables(tables);
    Path schemaPath = cacheDir.resolve("conceptual_schema.yaml");
    saveConceptualSchema(tablesOnlySchema, schemaPath);
    log.info("Saved schema to: {}", schemaPath);
    
    // Write endpoint index files
    for (String table : schemaResult.tables.keySet()) {
      List<String> endpointIds = schemaResult.getEndpoints(table);
      
      // Find actual endpoints matching these operationIds
      List<EndpointInfo> tableEndpoints = new ArrayList<>();
      for (EndpointInfo endpoint : allEndpoints) {
        if (endpointIds.contains(endpoint.operationId)) {
          tableEndpoints.add(endpoint);
        }
      }
      
      // Save endpoint index
      TableEndpointIndex index = new TableEndpointIndex(table, new ArrayList<>());
      for (EndpointInfo endpoint : tableEndpoints) {
        TableEndpointIndex.EndpointInfo endpointInfo = convertToEndpointInfo(endpoint);
        index.getEndpoints().add(endpointInfo);
      }
      
      Path tableFile = endpointInfoDir.resolve(table + ".json");
      String content = JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(index);
      Files.writeString(tableFile, content);
      log.debug("Saved endpoint index for table '{}' with {} endpoints", table, tableEndpoints.size());
    }

    // Phase 3: Column Extraction
    log.info("Phase 3: Extracting columns");
    ConceptualSchema finalSchema = extractColumns(allEndpoints, schemaResult, handbookPath);
    saveConceptualSchema(finalSchema, schemaPath);
    log.info("Updated schema with columns: {}", schemaPath);

    // Phase 4: Cache API preamble (API description + all documentation)
    log.info("Phase 4: Caching API preamble");
    String allDocsContent = loadAllDocumentation(handbookPath);
    savePreamble(openAPI, allDocsContent);
    log.info("Cached API preamble for transducer and planner");

    log.info("Successfully extracted schema: {} tables", finalSchema.getTables().size());
    return finalSchema;
  }

  /**
   * Result of OpenAPI extraction containing both endpoints and the OpenAPI object.
   */
  private static class OpenApiExtractionResult {
    final List<EndpointInfo> endpoints;
    final io.swagger.v3.oas.models.OpenAPI openAPI;

    OpenApiExtractionResult(List<EndpointInfo> endpoints, io.swagger.v3.oas.models.OpenAPI openAPI) {
      this.endpoints = endpoints;
      this.openAPI = openAPI;
    }
  }

  /**
   * Phase 1: Extract all endpoints from OpenAPI specifications.
   */
  private OpenApiExtractionResult extractAllEndpoints(Path handbookPath) throws IOException {
    Path apisDir = handbookPath.resolve("apis");
    Path openapiDir = handbookPath.resolve("openapi");
    Path sourceDir = null;
    if (Files.exists(apisDir)) {
      sourceDir = apisDir;
    } else if (Files.exists(openapiDir)) {
      sourceDir = openapiDir;
    } else {
      throw new IOException("APIs directory not found in: " + handbookPath);
    }

    // Load first OpenAPI file
    Path openApiFile = null;
    for (Path file : Files.walk(sourceDir)
        .filter(Files::isRegularFile)
        .filter(p -> {
          String name = p.getFileName().toString().toLowerCase();
          return name.endsWith(".yaml") || name.endsWith(".yml") || name.endsWith(".json");
        })
        .limit(1)
        .toList()) {
      openApiFile = file;
      break;
    }

    if (openApiFile == null) {
      throw new IOException("No OpenAPI specifications found in: " + sourceDir);
    }

    // Parse OpenAPI spec
    io.swagger.v3.parser.OpenAPIV3Parser parser = new io.swagger.v3.parser.OpenAPIV3Parser();
    io.swagger.v3.oas.models.OpenAPI openAPI = parser.read(openApiFile.toString());
    if (openAPI == null) {
      throw new IOException("Failed to parse OpenAPI file: " + openApiFile);
    }

    List<EndpointInfo> endpoints = extractEndpointsFromOpenAPI(openAPI);
    return new OpenApiExtractionResult(endpoints, openAPI);
  }

  /**
   * Extract endpoints from OpenAPI spec.
   */
  private List<EndpointInfo> extractEndpointsFromOpenAPI(io.swagger.v3.oas.models.OpenAPI openAPI) {
    List<EndpointInfo> endpoints = new ArrayList<>();

    if (openAPI.getPaths() == null) {
      return endpoints;
    }

    for (Map.Entry<String, io.swagger.v3.oas.models.PathItem> pathEntry :
        openAPI.getPaths().entrySet()) {
      String path = pathEntry.getKey();
      io.swagger.v3.oas.models.PathItem pathItem = pathEntry.getValue();

      Map<io.swagger.v3.oas.models.PathItem.HttpMethod,
          io.swagger.v3.oas.models.Operation> ops = pathItem.readOperationsMap();

      for (Map.Entry<io.swagger.v3.oas.models.PathItem.HttpMethod,
          io.swagger.v3.oas.models.Operation> opEntry : ops.entrySet()) {
        String method = opEntry.getKey().name();
        io.swagger.v3.oas.models.Operation operation = opEntry.getValue();

        if (operation.getOperationId() == null) {
          continue;
        }

        String requestSchema = extractRequestSchema(operation, openAPI);
        String responseSchema = extractResponseSchema(operation, openAPI);
        String parameters = extractParameters(operation);

        endpoints.add(new EndpointInfo(
            method,
            path,
            operation.getOperationId(),
            operation.getSummary() != null ? operation.getSummary() : "",
            operation.getDescription() != null ? operation.getDescription() : "",
            requestSchema,
            responseSchema,
            parameters));
      }
    }

    return endpoints;
  }

  /**
   * Phase 2: Generate schema with LLM.
   */
  private SchemaResult generateSchemaWithLLM(
      List<EndpointInfo> allEndpoints,
      io.swagger.v3.oas.models.OpenAPI openAPI,
      Path handbookPath)
      throws IOException, ExecutionException {
    
    String instructionsContent = "";
    Path instructionsPath = handbookPath.resolve("Agent.md");
    if (Files.exists(instructionsPath)) {
      try {
        instructionsContent = Files.readString(instructionsPath);
      } catch (Exception e) {
        log.debug("Could not load Agent.md", e);
      }
    }

    // Load API preamble from cache (API description + docs), or load separately if not cached
    String preamble = loadPreamble();
    String allDocsContent;
    String openApiInfoStr;
    
    if (preamble != null && !preamble.isEmpty()) {
      // Use cached API preamble
      log.debug("Using cached API preamble for transducer");
      // Split preamble into API description and docs sections
      String[] parts = preamble.split("\n\n---\n\n# Documentation\n\n", 2);
      if (parts.length >= 1) {
        // Extract API description (remove "# API Description" header)
        String apiSection = parts[0].replaceFirst("^# API Description\\s*\n\\s*", "");
        openApiInfoStr = apiSection.trim();
      } else {
        openApiInfoStr = "";
      }
      if (parts.length >= 2) {
        allDocsContent = parts[1].trim();
      } else {
        allDocsContent = "";
      }
    } else {
      // Fallback: load separately (for backward compatibility or if cache doesn't exist)
      log.debug("API preamble not found in cache, loading separately");
      allDocsContent = loadAllDocumentation(handbookPath);
      
      // Extract OpenAPI main description
      StringBuilder openApiInfo = new StringBuilder();
      if (openAPI.getInfo() != null) {
        if (openAPI.getInfo().getTitle() != null) {
          openApiInfo.append("Title: ").append(openAPI.getInfo().getTitle()).append("\n");
        }
        if (openAPI.getInfo().getDescription() != null) {
          openApiInfo.append("Description: ").append(openAPI.getInfo().getDescription()).append("\n");
        }
        if (openAPI.getInfo().getVersion() != null) {
          openApiInfo.append("Version: ").append(openAPI.getInfo().getVersion()).append("\n");
        }
      }
      openApiInfoStr = openApiInfo.toString();
    }

    // Build endpoint descriptions catalog
    StringBuilder descriptions = new StringBuilder();
    for (EndpointInfo endpoint : allEndpoints) {
      descriptions.append("Operation ID: ").append(endpoint.operationId).append("\n");
      descriptions.append("  Method: ").append(endpoint.method).append(" ").append(endpoint.path).append("\n");
      if (!endpoint.summary.isEmpty()) {
        descriptions.append("  Summary: ").append(endpoint.summary).append("\n");
      }
      if (!endpoint.description.isEmpty()) {
        descriptions.append("  Description: ").append(endpoint.description).append("\n");
      }
      descriptions.append("\n");
    }

    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("transducer-schema");

    Map<String, Object> context = new HashMap<>();
    context.put("instructions_content", instructionsContent);
    context.put("documentation", allDocsContent);
    context.put("openapi_info", openApiInfoStr);
    context.put("endpoint_descriptions", descriptions.toString());

    PromptTemplate.PromptSession session = template.newSession();
    session.enable("transducer-schema", context);

    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      throw new ExecutionException("Failed to render transducer schema prompt: " + e.getMessage(), e);
    }

    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from transducer schema prompt");
    }

    // Create telemetry sink with phase name
    final Map<String, Object> sinkAttributes = new HashMap<>();
    sinkAttributes.put("phase", "transducer-schema");
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
          public void addUsage(Long promptTokens, Long completionTokens, Long totalTokens) {}
          @Override
          public java.util.Map<String, Object> currentAttributes() {
            return sinkAttributes;
          }
        };

    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, null);
    }

    String jsonStr = extractJsonFromResponse(response);
    if (jsonStr == null || jsonStr.trim().isEmpty()) {
      throw new ExecutionException("No JSON content found in LLM response");
    }

    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = objectMapper.readValue(jsonStr, Map.class);

      @SuppressWarnings("unchecked")
      Map<String, Object> tables = (Map<String, Object>) resultMap.get("tables");

      if (tables == null) {
        throw new ExecutionException("LLM response missing 'tables' field");
      }

      return new SchemaResult(tables);
    } catch (Exception e) {
      throw new ExecutionException("Failed to parse schema result: " + e.getMessage(), e);
    }
  }

  /**
   * Phase 3: Extract columns from endpoints and assign to tables.
   * First tries to extract from data-model.md documentation, then falls back to OpenAPI schema.
   */
  private ConceptualSchema extractColumns(
      List<EndpointInfo> allEndpoints,
      SchemaResult schemaResult,
      Path handbookPath) throws IOException {
    
    ConceptualSchema schema = new ConceptualSchema();
    Map<String, ConceptualSchema.TableDefinition> tables = new HashMap<>();
    
    // Try to load field definitions from data-model.md
    Map<String, Set<String>> fieldsFromDocs = loadFieldsFromDataModel(handbookPath);
    
    // Check if LLM generated columns - if so, use them and skip extraction
    boolean hasLlmColumns = false;
    for (String table : schemaResult.tables.keySet()) {
      List<String> llmColumns = schemaResult.getColumns(table);
      if (llmColumns != null && !llmColumns.isEmpty()) {
        hasLlmColumns = true;
        break;
      }
    }
    
    if (hasLlmColumns) {
      log.info("Using LLM-generated columns, skipping column extraction");
      // Use LLM-generated columns directly
      for (String table : schemaResult.tables.keySet()) {
        List<String> llmColumns = schemaResult.getColumns(table);
        if (llmColumns != null && !llmColumns.isEmpty()) {
          List<String> columnList = new ArrayList<>(llmColumns);
          java.util.Collections.sort(columnList);
          ConceptualSchema.TableDefinition tableDef = new ConceptualSchema.TableDefinition(columnList);
          tables.put(table, tableDef);
        }
      }
      schema.setTables(tables);
      return schema;
    }
    
    // Map endpoints to tables
    Map<String, List<EndpointInfo>> tableToEndpoints = new HashMap<>();
    for (String table : schemaResult.tables.keySet()) {
      List<String> endpointIds = schemaResult.getEndpoints(table);
      
      List<EndpointInfo> tableEndpoints = new ArrayList<>();
      for (EndpointInfo endpoint : allEndpoints) {
        if (endpointIds.contains(endpoint.operationId)) {
          tableEndpoints.add(endpoint);
        }
      }
      tableToEndpoints.put(table, tableEndpoints);
    }
    
    // Extract columns for each table
    for (Map.Entry<String, List<EndpointInfo>> entry : tableToEndpoints.entrySet()) {
      String table = entry.getKey();
      List<EndpointInfo> endpoints = entry.getValue();
      
      Set<String> columns = new HashSet<>();
      
      // First, try to get fields from data-model.md documentation
      Set<String> docFields = fieldsFromDocs.get(table);
      if (docFields != null && !docFields.isEmpty()) {
        columns.addAll(docFields);
        log.debug("Extracted {} columns for table '{}' from data-model.md", columns.size(), table);
      } else {
        // Fall back to extracting from OpenAPI schema
        for (EndpointInfo endpoint : endpoints) {
          // Skip metadata endpoints (like getAvailableFields) - they return field definitions, not data columns
          // These endpoints describe the schema but don't contain actual data fields
          if (endpoint.operationId != null && 
              (endpoint.operationId.contains("Fields") || 
               endpoint.operationId.contains("Schema") ||
               endpoint.operationId.equals("getAvailableFields"))) {
            continue;
          }
          
          // Skip request schema - it contains query parameters (filters, aggregates, etc.) not data columns
          // Only extract from response schema, and only from actual data fields (skip wrappers)
          extractColumnsFromSchema(endpoint.responseSchema, columns);
          // Skip parameters - they're query parameters, not data columns
        }
        log.debug("Extracted {} columns for table '{}' from OpenAPI schema", columns.size(), table);
      }
      
      List<String> columnList = new ArrayList<>(columns);
      java.util.Collections.sort(columnList);
      
      ConceptualSchema.TableDefinition tableDef = new ConceptualSchema.TableDefinition(columnList);
      tables.put(table, tableDef);
    }
    
    schema.setTables(tables);
    return schema;
  }
  
  /**
   * Load field definitions from data-model.md documentation.
   * Returns a map from table name to set of column names (with type suffixes).
   */
  private Map<String, Set<String>> loadFieldsFromDataModel(Path handbookPath) {
    Map<String, Set<String>> fieldsByTable = new HashMap<>();
    
    try {
      Path dataModelPath = handbookPath.resolve("docs").resolve("data-model.md");
      if (!Files.exists(dataModelPath)) {
        log.debug("data-model.md not found at: {}", dataModelPath);
        return fieldsByTable;
      }
      
      String content = Files.readString(dataModelPath);
      log.info("Loading field definitions from: {}", dataModelPath);
      
      // Parse markdown to extract field definitions
      // Look for sections like "### Sale Fields", "### Product Fields", etc.
      String[] lines = content.split("\n");
      String currentEntity = null;
      boolean inFieldTable = false;
      
      for (String line : lines) {
        // Check for entity section headers (e.g., "### Sale Fields", "### Product Fields")
        if (line.startsWith("### ")) {
          String section = line.substring(4).trim();
          if (section.endsWith(" Fields")) {
            currentEntity = section.substring(0, section.length() - 7).toLowerCase(); // "Sale Fields" -> "sale"
            inFieldTable = false;
            log.debug("Found entity section: {}", currentEntity);
          } else {
            currentEntity = null;
            inFieldTable = false;
          }
        }
        // Check for markdown table start (line with | Field | Type | ...)
        else if (line.contains("| Field |") && currentEntity != null) {
          inFieldTable = true;
        }
        // Parse table rows (skip header separator line)
        else if (inFieldTable && currentEntity != null && line.startsWith("|") && 
                 !line.contains("---") && line.contains("`")) {
          // Parse row like: | `sale.id` | string | Unique sale identifier | "SAL-001" |
          // Or: | `customer.state` | string | Customer state/province | "CA" |
          String[] parts = line.split("\\|");
          if (parts.length >= 3) {
            String fieldCell = parts[1].trim();
            String typeCell = parts.length > 2 ? parts[2].trim() : "";
            String descCell = parts.length > 3 ? parts[3].trim() : "";
            String exampleCell = parts.length > 4 ? parts[4].trim() : "";
            
            // Extract field name from backticks (e.g., `sale.id` -> sale.id)
            if (fieldCell.startsWith("`") && fieldCell.endsWith("`")) {
              String fieldName = fieldCell.substring(1, fieldCell.length() - 1);
              
              // Convert field name to column name format with format-aware type suffix
              // e.g., "customer.state" with example "CA" -> "state_code_str" (2-char code)
              // e.g., "sale.id" -> "id_str", "sale.amount" -> "amount_int"
              String columnName = convertFieldNameToColumnName(fieldName, typeCell, descCell, exampleCell);
              
              fieldsByTable.computeIfAbsent(currentEntity, k -> new HashSet<>()).add(columnName);
              log.debug("Extracted field: {} -> {} (type: {}, desc: {}, example: {})", 
                  fieldName, columnName, typeCell, descCell, exampleCell);
            }
          }
        }
        // End of table (empty line or new section)
        else if (inFieldTable && (line.trim().isEmpty() || line.startsWith("##"))) {
          inFieldTable = false;
        }
      }
      
      log.info("Loaded field definitions for {} entities from data-model.md", fieldsByTable.size());
      
    } catch (Exception e) {
      log.warn("Failed to load fields from data-model.md: {}", e.getMessage());
    }
    
    return fieldsByTable;
  }
  
  /**
   * Load API preamble (API description + docs) from cache file.
   * The preamble contains the OpenAPI main description and all documentation from docs/.
   * Returns null if file doesn't exist.
   */
  private String loadPreamble() {
    try {
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
  private String loadAllDocumentation(Path handbookPath) {
    StringBuilder allDocs = new StringBuilder();
    
    try {
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
          log.info("Loaded {} documentation file(s) from: {}", docFiles.size(), docsPath);
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
   * Convert field name (e.g., "sale.id", "sale.amount") to column name format (e.g., "id_str", "amount_int").
   * Removes entity prefix and adds type suffix with format awareness.
   * Uses description and example to infer format constraints (e.g., 2-char codes, email, phone).
   */
  private String convertFieldNameToColumnName(String fieldName, String typeStr, String description, String example) {
    // Remove entity prefix (e.g., "sale.id" -> "id", "product.name" -> "name")
    String field = fieldName;
    int dotIndex = fieldName.indexOf('.');
    if (dotIndex > 0) {
      field = fieldName.substring(dotIndex + 1);
    }
    
    // Convert to lowercase and replace non-alphanumeric with underscores
    String columnName = field.toLowerCase().replaceAll("[^a-z0-9]+", "_");
    
    // Analyze description and example to infer format constraints
    String descLower = description != null ? description.toLowerCase() : "";
    String exampleStr = example != null ? example.trim() : "";
    // Remove quotes from example if present
    if (exampleStr.startsWith("\"") && exampleStr.endsWith("\"")) {
      exampleStr = exampleStr.substring(1, exampleStr.length() - 1);
    }
    
    // Add type suffix based on type string and format constraints
    String typeLower = typeStr != null ? typeStr.toLowerCase() : "";
    
    if (typeLower.contains("number") || typeLower.contains("integer")) {
      columnName += "_int";
    } else if (typeLower.contains("boolean")) {
      columnName += "_bool";
    } else if (typeLower.contains("date") || typeLower.contains("datetime")) {
      columnName += "_datetime_str"; // More specific for dates
    } else if (typeLower.contains("array")) {
      columnName += "_arr";
    } else if (typeLower.contains("object")) {
      columnName += "_obj";
    } else {
      // String type - check for format constraints
      if (descLower.contains("email") || exampleStr.contains("@")) {
        columnName += "_email_str";
      } else if (descLower.contains("phone") || descLower.contains("telephone") || 
                 exampleStr.matches(".*[\\d\\-\\+\\(\\)].*")) {
        columnName += "_phone_str";
      } else if (descLower.contains("zip") || descLower.contains("postal")) {
        columnName += "_zip_str";
      } else if (descLower.contains("state") || descLower.contains("province")) {
        // Check if example suggests 2-char code (e.g., "CA", "NY")
        if (exampleStr.length() == 2 && exampleStr.matches("[A-Z]{2}")) {
          columnName += "_code_str"; // 2-char state code
        } else {
          columnName += "_str";
        }
      } else if (descLower.contains("code") || descLower.contains("abbreviation") || 
                 descLower.contains("abbr")) {
        // Check example length for code format
        if (exampleStr.length() <= 5 && exampleStr.matches("[A-Z0-9]+")) {
          columnName += "_code_str";
        } else {
          columnName += "_str";
        }
      } else if (descLower.contains("id") || descLower.contains("identifier")) {
        columnName += "_id_str";
      } else if (descLower.contains("url") || descLower.contains("uri") || 
                 exampleStr.startsWith("http")) {
        columnName += "_url_str";
      } else if (exampleStr.length() == 2 && exampleStr.matches("[A-Z]{2}")) {
        // Generic 2-char uppercase code (e.g., state, country code)
        columnName += "_code_str";
      } else {
        columnName += "_str";
      }
    }
    
    return columnName;
  }

  /**
   * Extract columns from JSON schema string.
   */
  private void extractColumnsFromSchema(String schemaJson, Set<String> columns) {
    if (schemaJson == null || schemaJson.trim().isEmpty()) {
      return;
    }

    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> schema = objectMapper.readValue(schemaJson, Map.class);
      extractColumnsFromSchemaMap(schema, "", columns);
    } catch (Exception e) {
      log.debug("Failed to parse schema for column extraction", e);
    }
  }

  /**
   * Recursively extract columns from schema map.
   * Excludes response wrapper fields (success, data, metadata) and extracts only actual data columns.
   */
  private void extractColumnsFromSchemaMap(Map<String, Object> schema, String prefix, Set<String> columns) {
    if (schema == null) {
      return;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
    if (properties != null) {
      for (Map.Entry<String, Object> prop : properties.entrySet()) {
        String fieldName = prop.getKey();
        
        // Skip response wrapper fields - these are not data columns
        if ("success".equals(fieldName) || "error".equals(fieldName) || "message".equals(fieldName)) {
          continue;
        }
        
        // For "data" array field, check if it has actual field definitions
        // Query endpoints often have data arrays with additionalProperties: true,
        // meaning the actual fields aren't in the schema - skip these
        if ("data".equals(fieldName)) {
          @SuppressWarnings("unchecked")
          Map<String, Object> propSchema = (Map<String, Object>) prop.getValue();
          if (propSchema != null) {
            String type = (String) propSchema.get("type");
            if ("array".equals(type)) {
              // Extract columns from array items only if they have actual properties defined
              @SuppressWarnings("unchecked")
              Map<String, Object> items = (Map<String, Object>) propSchema.get("items");
              if (items != null) {
                // Check if items have actual properties (not just additionalProperties: true)
                @SuppressWarnings("unchecked")
                Map<String, Object> itemProperties = (Map<String, Object>) items.get("properties");
                Boolean additionalProperties = (Boolean) items.get("additionalProperties");
                
                // Only extract if there are actual properties defined
                // If additionalProperties is true and no properties, skip (dynamic schema)
                if (itemProperties != null && !itemProperties.isEmpty()) {
                  extractColumnsFromSchemaMap(items, prefix, columns);
                }
                // If additionalProperties is true, the fields are dynamic - don't extract
              }
            }
          }
          continue; // Don't add "data" itself as a column
        }
        
        // Skip "metadata" object - it's a response wrapper, not data
        // Also skip if prefix indicates we're inside a metadata object
        if ("metadata".equals(fieldName) || "metadata".equals(prefix) || 
            (prefix != null && prefix.startsWith("metadata_"))) {
          continue;
        }
        
        String columnName = prefix.isEmpty() ? fieldName : prefix + "_" + fieldName;
        columnName = columnName.toLowerCase().replaceAll("[^a-z0-9]+", "_");
        
        // Add type suffix based on schema type and format constraints
        @SuppressWarnings("unchecked")
        Map<String, Object> propSchema = (Map<String, Object>) prop.getValue();
        if (propSchema != null) {
          String type = (String) propSchema.get("type");
          String format = (String) propSchema.get("format");
          String description = (String) propSchema.get("description");
          Object example = propSchema.get("example");
          String exampleStr = example != null ? example.toString() : "";
          
          if ("string".equals(type)) {
            // Check format and description for more specific suffixes
            if ("email".equals(format) || (description != null && description.toLowerCase().contains("email"))) {
              columnName += "_email_str";
            } else if ("uri".equals(format) || "url".equals(format) || 
                       (description != null && (description.toLowerCase().contains("url") || 
                        description.toLowerCase().contains("uri")))) {
              columnName += "_url_str";
            } else if (format != null && (format.contains("date") || format.contains("time"))) {
              columnName += "_datetime_str";
            } else if (description != null) {
              String descLower = description.toLowerCase();
              if (descLower.contains("phone") || descLower.contains("telephone")) {
                columnName += "_phone_str";
              } else if (descLower.contains("zip") || descLower.contains("postal")) {
                columnName += "_zip_str";
              } else if (descLower.contains("state") || descLower.contains("province")) {
                // Check example for 2-char code pattern (e.g., "CA", "NY")
                if (exampleStr.length() == 2 && exampleStr.matches("[A-Z]{2}")) {
                  columnName += "_code_str";
                } else {
                  columnName += "_str";
                }
              } else if (descLower.contains("code") || descLower.contains("abbreviation") || 
                         descLower.contains("abbr")) {
                // Check example length for code format (typically 2-5 chars, uppercase alphanumeric)
                if (exampleStr.length() <= 5 && exampleStr.matches("[A-Z0-9]+")) {
                  columnName += "_code_str";
                } else {
                  columnName += "_str";
                }
              } else if (descLower.contains("id") || descLower.contains("identifier")) {
                columnName += "_id_str";
              } else {
                columnName += "_str";
              }
            } else if (exampleStr.length() == 2 && exampleStr.matches("[A-Z]{2}")) {
              // Generic 2-char uppercase code pattern (e.g., state, country code)
              columnName += "_code_str";
            } else {
              columnName += "_str";
            }
          } else if ("integer".equals(type) || "number".equals(type)) {
            columnName += "_int";
          } else if ("boolean".equals(type)) {
            columnName += "_bool";
          } else if ("array".equals(type)) {
            columnName += "_arr";
          } else if ("object".equals(type)) {
            columnName += "_obj";
          }
        }
        
        columns.add(columnName);
        
        // Recursively process nested objects (but skip metadata and response wrappers)
        if (propSchema != null && !"metadata".equals(fieldName) && 
            !"success".equals(fieldName) && !"data".equals(fieldName) &&
            !"metadata".equals(prefix) && 
            (prefix == null || !prefix.startsWith("metadata_"))) {
          extractColumnsFromSchemaMap(propSchema, columnName, columns);
        }
      }
    }
  }

  /**
   * Extract columns from parameters JSON string.
   */
  private void extractColumnsFromParameters(String parametersJson, Set<String> columns) {
    if (parametersJson == null || parametersJson.trim().isEmpty()) {
      return;
    }

    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> params = objectMapper.readValue(parametersJson, Map.class);
      for (String paramName : params.keySet()) {
        String columnName = paramName.toLowerCase().replaceAll("[^a-z0-9]+", "_") + "_str";
        columns.add(columnName);
      }
    } catch (Exception e) {
      log.debug("Failed to parse parameters for column extraction", e);
    }
  }

  /**
   * Convert EndpointInfo to TableEndpointIndex.EndpointInfo.
   */
  private TableEndpointIndex.EndpointInfo convertToEndpointInfo(EndpointInfo endpoint) {
    TableEndpointIndex.EndpointInfo info = new TableEndpointIndex.EndpointInfo();
    info.setMethod(endpoint.method);
    info.setPath(endpoint.path);
    info.setOperationId(endpoint.operationId);
    info.setSummary(endpoint.summary);
    info.setDescription(endpoint.description);

    if (endpoint.parameters != null && !endpoint.parameters.isEmpty()) {
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> params = objectMapper.readValue(endpoint.parameters, Map.class);
        info.setParams(params);
      } catch (Exception e) {
        log.debug("Failed to parse parameters", e);
      }
    }

    if (endpoint.requestSchema != null && !endpoint.requestSchema.isEmpty()) {
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = objectMapper.readValue(endpoint.requestSchema, Map.class);
        info.setRequestSchema(schema);
      } catch (Exception e) {
        log.debug("Failed to parse request schema", e);
      }
    }

    if (endpoint.responseSchema != null && !endpoint.responseSchema.isEmpty()) {
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = objectMapper.readValue(endpoint.responseSchema, Map.class);
        info.setResponseSchema(schema);
      } catch (Exception e) {
        log.debug("Failed to parse response schema", e);
      }
    }

    return info;
  }

  // Schema extraction helpers
  private String extractRequestSchema(
      io.swagger.v3.oas.models.Operation operation,
      io.swagger.v3.oas.models.OpenAPI openAPI) {
    if (operation.getRequestBody() == null ||
        operation.getRequestBody().getContent() == null) {
      return null;
    }

    io.swagger.v3.oas.models.media.Content content = operation.getRequestBody().getContent();
    io.swagger.v3.oas.models.media.MediaType jsonMediaType = content.get("application/json");
    if (jsonMediaType == null && !content.isEmpty()) {
      jsonMediaType = content.values().iterator().next();
    }

    if (jsonMediaType != null && jsonMediaType.getSchema() != null) {
      return serializeSchema(jsonMediaType.getSchema(), openAPI);
    }

    return null;
  }

  private String extractResponseSchema(
      io.swagger.v3.oas.models.Operation operation,
      io.swagger.v3.oas.models.OpenAPI openAPI) {
    if (operation.getResponses() == null || operation.getResponses().isEmpty()) {
      return null;
    }

    io.swagger.v3.oas.models.responses.ApiResponse response = null;
    for (Map.Entry<String, io.swagger.v3.oas.models.responses.ApiResponse> entry :
        operation.getResponses().entrySet()) {
      String statusCode = entry.getKey();
      if (statusCode.startsWith("2") || "default".equals(statusCode)) {
        response = entry.getValue();
        break;
      }
    }

    if (response == null || response.getContent() == null) {
      return null;
    }

    io.swagger.v3.oas.models.media.Content content = response.getContent();
    io.swagger.v3.oas.models.media.MediaType jsonMediaType = content.get("application/json");
    if (jsonMediaType == null && !content.isEmpty()) {
      jsonMediaType = content.values().iterator().next();
    }

    if (jsonMediaType != null && jsonMediaType.getSchema() != null) {
      return serializeSchema(jsonMediaType.getSchema(), openAPI);
    }

    return null;
  }

  private String extractParameters(io.swagger.v3.oas.models.Operation operation) {
    if (operation.getParameters() == null || operation.getParameters().isEmpty()) {
      return null;
    }

    try {
      Map<String, Object> paramsMap = new HashMap<>();
      for (io.swagger.v3.oas.models.parameters.Parameter param : operation.getParameters()) {
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("name", param.getName());
        paramMap.put("in", param.getIn());
        paramMap.put("required", param.getRequired() != null && param.getRequired());
        if (param.getSchema() != null) {
          paramMap.put("type", param.getSchema().getType());
        }
        paramsMap.put(param.getName(), paramMap);
      }
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(paramsMap);
    } catch (Exception e) {
      log.debug("Failed to serialize parameters", e);
      return null;
    }
  }

  private String serializeSchema(
      io.swagger.v3.oas.models.media.Schema<?> schema,
      io.swagger.v3.oas.models.OpenAPI openAPI) {
    try {
      Map<String, Object> schemaMap = serializeSchemaToMap(schema, openAPI, new HashSet<>());
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schemaMap);
    } catch (Exception e) {
      log.debug("Failed to serialize schema", e);
      return null;
    }
  }

  private Map<String, Object> serializeSchemaToMap(
      io.swagger.v3.oas.models.media.Schema<?> schema,
      io.swagger.v3.oas.models.OpenAPI openAPI,
      Set<String> seenRefs) {
    if (schema == null) {
      return new HashMap<>();
    }

    if (schema.get$ref() != null) {
      String refPath = schema.get$ref();
      if (refPath.startsWith("#/components/schemas/")) {
        String schemaName = refPath.substring("#/components/schemas/".length());
        if (seenRefs.contains(schemaName)) {
          Map<String, Object> refMap = new HashMap<>();
          refMap.put("$ref", refPath);
          return refMap;
        }
        seenRefs.add(schemaName);

        if (openAPI.getComponents() != null &&
            openAPI.getComponents().getSchemas() != null) {
          io.swagger.v3.oas.models.media.Schema<?> resolved =
              openAPI.getComponents().getSchemas().get(schemaName);
          if (resolved != null) {
            return serializeSchemaToMap(resolved, openAPI, seenRefs);
          }
        }
      }
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
      for (Map.Entry<String, io.swagger.v3.oas.models.media.Schema> propEntry :
          schema.getProperties().entrySet()) {
        propertiesMap.put(propEntry.getKey(),
            serializeSchemaToMap(propEntry.getValue(), openAPI, seenRefs));
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
      schemaMap.put("items", serializeSchemaToMap(schema.getItems(), openAPI, seenRefs));
    }

    return schemaMap;
  }

  // Helper methods
  private void saveEndpointDescriptions(List<EndpointInfo> endpoints, Path outputPath) throws IOException {
    List<Map<String, Object>> descriptions = new ArrayList<>();
    for (EndpointInfo endpoint : endpoints) {
      Map<String, Object> desc = new HashMap<>();
      desc.put("method", endpoint.method);
      desc.put("path", endpoint.path);
      desc.put("operation_id", endpoint.operationId);
      desc.put("summary", endpoint.summary);
      desc.put("description", endpoint.description);
      descriptions.add(desc);
    }
    String content = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(descriptions);
    Files.writeString(outputPath, content);
  }

  private void saveConceptualSchema(ConceptualSchema schema, Path outputPath) throws IOException {
    Files.createDirectories(outputPath.getParent());
    JacksonUtility.getYamlMapper().writeValue(outputPath.toFile(), schema);
  }

  /**
   * Save OpenAPI description to cache for easy access by planner.
   */
  private void saveOpenApiDescription(io.swagger.v3.oas.models.OpenAPI openAPI) throws IOException {
    Path openApiInfoFile = cacheDir.resolve("openapi_info.json");
    
    Map<String, Object> info = new HashMap<>();
    if (openAPI.getInfo() != null) {
      if (openAPI.getInfo().getTitle() != null) {
        info.put("title", openAPI.getInfo().getTitle());
      }
      if (openAPI.getInfo().getDescription() != null) {
        info.put("description", openAPI.getInfo().getDescription());
      }
      if (openAPI.getInfo().getVersion() != null) {
        info.put("version", openAPI.getInfo().getVersion());
      }
    }
    
    String content = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(info);
    Files.writeString(openApiInfoFile, content);
    log.debug("Saved OpenAPI description to: {}", openApiInfoFile);
  }

  /**
   * Save API preamble to cache file.
   * The preamble contains the OpenAPI main description followed by all documentation from docs/ directory.
   * This preamble is combined with table-specific endpoints to form the full api_context(table_name) for the planner.
   */
  private void savePreamble(io.swagger.v3.oas.models.OpenAPI openAPI, String allDocsContent) throws IOException {
    Path preambleFile = cacheDir.resolve("api_preamble.md");
    
    StringBuilder preamble = new StringBuilder();
    preamble.append("# API Description\n\n");
    
    // Add OpenAPI main description
    if (openAPI.getInfo() != null) {
      if (openAPI.getInfo().getTitle() != null) {
        preamble.append("**Title:** ").append(openAPI.getInfo().getTitle()).append("\n\n");
      }
      if (openAPI.getInfo().getDescription() != null) {
        preamble.append("**Description:**\n\n").append(openAPI.getInfo().getDescription()).append("\n\n");
      }
      if (openAPI.getInfo().getVersion() != null) {
        preamble.append("**Version:** ").append(openAPI.getInfo().getVersion()).append("\n\n");
      }
    }
    
    preamble.append("\n\n---\n\n# Documentation\n\n");
    
    // Add all documentation
    if (allDocsContent != null && !allDocsContent.trim().isEmpty()) {
      preamble.append(allDocsContent);
    } else {
      preamble.append("(No documentation files found in docs/ directory)\n");
    }
    
    Files.writeString(preambleFile, preamble.toString());
    log.debug("Saved API preamble to: {}", preambleFile);
  }

  private String extractJsonFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String jsonStr = response.trim();
    if (jsonStr.startsWith("```json")) {
      jsonStr = jsonStr.substring(7).trim();
    } else if (jsonStr.startsWith("```")) {
      jsonStr = jsonStr.substring(3).trim();
    }

    if (jsonStr.endsWith("```")) {
      jsonStr = jsonStr.substring(0, jsonStr.length() - 3).trim();
    }

    int firstBrace = jsonStr.indexOf('{');
    int lastBrace = jsonStr.lastIndexOf('}');

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      jsonStr = jsonStr.substring(firstBrace, lastBrace + 1);
    }

    return jsonStr.trim();
  }

  // Data classes
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class SchemaResult {
    @JsonProperty("tables")
    Map<String, Object> tables; // Can be List<String> (endpoints) or Map with "endpoints" and "columns"

    public SchemaResult() {}

    public SchemaResult(Map<String, Object> tables) {
      this.tables = tables != null ? tables : new HashMap<>();
    }
    
    // Helper to extract endpoints list (handles both old and new format)
    public List<String> getEndpoints(String tableName) {
      Object tableData = tables.get(tableName);
      if (tableData == null) {
        return new ArrayList<>();
      }
      if (tableData instanceof List) {
        // Old format: ["getRepo", "listUserRepos"]
        @SuppressWarnings("unchecked")
        List<String> endpoints = (List<String>) tableData;
        return endpoints;
      } else if (tableData instanceof Map) {
        // New format: {"endpoints": [...], "columns": [...]}
        @SuppressWarnings("unchecked")
        Map<String, Object> tableMap = (Map<String, Object>) tableData;
        Object endpointsObj = tableMap.get("endpoints");
        if (endpointsObj instanceof List) {
          @SuppressWarnings("unchecked")
          List<String> endpoints = (List<String>) endpointsObj;
          return endpoints;
        }
      }
      return new ArrayList<>();
    }
    
    // Helper to extract columns list (if LLM generated them)
    public List<String> getColumns(String tableName) {
      Object tableData = tables.get(tableName);
      if (tableData == null) {
        return null; // null means columns not generated by LLM, use extraction
      }
      if (tableData instanceof Map) {
        // New format: {"endpoints": [...], "columns": [...]}
        @SuppressWarnings("unchecked")
        Map<String, Object> tableMap = (Map<String, Object>) tableData;
        Object columnsObj = tableMap.get("columns");
        if (columnsObj instanceof List) {
          @SuppressWarnings("unchecked")
          List<String> columns = (List<String>) columnsObj;
          return columns;
        }
      }
      return null; // null means columns not generated by LLM, use extraction
    }
  }

  private static class EndpointInfo {
    final String method;
    final String path;
    final String operationId;
    final String summary;
    final String description;
    final String requestSchema;
    final String responseSchema;
    final String parameters;

    EndpointInfo(String method, String path, String operationId, String summary,
        String description, String requestSchema, String responseSchema, String parameters) {
      this.method = method;
      this.path = path;
      this.operationId = operationId;
      this.summary = summary;
      this.description = description;
      this.requestSchema = requestSchema;
      this.responseSchema = responseSchema;
      this.parameters = parameters;
    }
  }
}
