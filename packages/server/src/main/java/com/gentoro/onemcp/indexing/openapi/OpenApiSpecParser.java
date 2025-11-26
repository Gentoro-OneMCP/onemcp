package com.gentoro.onemcp.indexing.openapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.utility.JacksonUtility;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.examples.Example;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.tags.Tag;
import java.util.*;

/**
 * Parser for OpenAPI specifications that extracts structured information for graph construction.
 *
 * <p>This parser extracts operations, schemas, tags, and examples from OpenAPI specs without
 * requiring the full spec to be sent to an LLM, enabling efficient processing of large
 * specifications.
 */
public class OpenApiSpecParser {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OpenApiSpecParser.class);

  private final OpenAPI openAPI;
  private final ObjectMapper jsonMapper;

  public OpenApiSpecParser(OpenAPI openAPI) {
    this.openAPI = openAPI;
    this.jsonMapper = JacksonUtility.getJsonMapper();
  }

  /** Represents a parsed operation from an OpenAPI spec. */
  public static class ParsedOperation {
    private final String operationId;
    private final String method;
    private final String path;
    private final String summary;
    private final String description;
    private final List<String> tags;
    private final String requestSchema;
    private final String responseSchema;
    private final List<ParsedExample> examples;
    private final List<Parameter> parameters;
    private final String category;

    public ParsedOperation(
        String operationId,
        String method,
        String path,
        String summary,
        String description,
        List<String> tags,
        String requestSchema,
        String responseSchema,
        List<ParsedExample> examples,
        List<Parameter> parameters,
        String category) {
      this.operationId = operationId;
      this.method = method;
      this.path = path;
      this.summary = summary;
      this.description = description;
      this.tags = tags != null ? tags : Collections.emptyList();
      this.requestSchema = requestSchema;
      this.responseSchema = responseSchema;
      this.examples = examples != null ? examples : Collections.emptyList();
      this.parameters = parameters != null ? parameters : Collections.emptyList();
      this.category = category;
    }

    public String getOperationId() {
      return operationId;
    }

    public String getMethod() {
      return method;
    }

    public String getPath() {
      return path;
    }

    public String getSummary() {
      return summary;
    }

    public String getDescription() {
      return description;
    }

    public List<String> getTags() {
      return tags;
    }

    public String getRequestSchema() {
      return requestSchema;
    }

    public String getResponseSchema() {
      return responseSchema;
    }

    public List<ParsedExample> getExamples() {
      return examples;
    }

    public List<Parameter> getParameters() {
      return parameters;
    }

    public String getCategory() {
      return category;
    }
  }

  /** Represents a parsed example from an OpenAPI spec. */
  public static class ParsedExample {
    private final String name;
    private final String summary;
    private final String description;
    private final String requestBody;
    private final String responseBody;
    private final String responseStatus;

    public ParsedExample(
        String name,
        String summary,
        String description,
        String requestBody,
        String responseBody,
        String responseStatus) {
      this.name = name;
      this.summary = summary;
      this.description = description;
      this.requestBody = requestBody;
      this.responseBody = responseBody;
      this.responseStatus = responseStatus;
    }

    public String getName() {
      return name;
    }

    public String getSummary() {
      return summary;
    }

    public String getDescription() {
      return description;
    }

    public String getRequestBody() {
      return requestBody;
    }

    public String getResponseBody() {
      return responseBody;
    }

    public String getResponseStatus() {
      return responseStatus;
    }
  }

  /** Represents a parsed tag/entity from an OpenAPI spec. */
  public static class ParsedTag {
    private final String name;
    private final String description;
    private final Map<String, String> externalDocs;

    public ParsedTag(String name, String description, Map<String, String> externalDocs) {
      this.name = name;
      this.description = description;
      this.externalDocs = externalDocs;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public Map<String, String> getExternalDocs() {
      return externalDocs;
    }
  }

  /** Represents a parsed schema component from an OpenAPI spec. */
  public static class ParsedSchema {
    private final String name;
    private final Schema<?> schema;
    private final String description;

    public ParsedSchema(String name, Schema<?> schema, String description) {
      this.name = name;
      this.schema = schema;
      this.description = description;
    }

    public String getName() {
      return name;
    }

    public Schema<?> getSchema() {
      return schema;
    }

    public String getDescription() {
      return description;
    }
  }

  /**
   * Extract all operations from the OpenAPI spec.
   *
   * @return list of parsed operations
   */
  public List<ParsedOperation> extractOperations() {
    List<ParsedOperation> operations = new ArrayList<>();

    if (openAPI.getPaths() == null) {
      return operations;
    }

    for (Map.Entry<String, PathItem> pathEntry : openAPI.getPaths().entrySet()) {
      String path = pathEntry.getKey();
      PathItem pathItem = pathEntry.getValue();

      Map<PathItem.HttpMethod, Operation> ops = pathItem.readOperationsMap();
      for (Map.Entry<PathItem.HttpMethod, Operation> opEntry : ops.entrySet()) {
        PathItem.HttpMethod httpMethod = opEntry.getKey();
        Operation operation = opEntry.getValue();

        if (operation.getOperationId() == null) {
          log.debug("Skipping operation without operationId: {} {}", httpMethod, path);
          continue;
        }

        ParsedOperation parsedOp = parseOperation(operation, httpMethod.name(), path);
        operations.add(parsedOp);
      }
    }

    return operations;
  }

  /** Parse a single operation from the OpenAPI spec. */
  private ParsedOperation parseOperation(Operation operation, String method, String path) {
    String operationId = operation.getOperationId();
    String summary = operation.getSummary() != null ? operation.getSummary() : "";
    String description = operation.getDescription() != null ? operation.getDescription() : summary;

    // Extract tags
    List<String> tags =
        operation.getTags() != null
            ? new ArrayList<>(operation.getTags())
            : Collections.emptyList();

    // Extract request schema
    String requestSchema = extractRequestSchema(operation);

    // Extract response schema
    String responseSchema = extractResponseSchema(operation);

    // Extract examples
    List<ParsedExample> examples = extractExamples(operation);

    // Extract parameters
    List<Parameter> parameters =
        operation.getParameters() != null
            ? new ArrayList<>(operation.getParameters())
            : Collections.emptyList();

    // Determine category based on HTTP method
    String category = determineCategory(method, operation);

    return new ParsedOperation(
        operationId,
        method,
        path,
        summary,
        description,
        tags,
        requestSchema,
        responseSchema,
        examples,
        parameters,
        category);
  }

  /** Extract request schema from operation (request body). */
  private String extractRequestSchema(Operation operation) {
    if (operation.getRequestBody() == null || operation.getRequestBody().getContent() == null) {
      return null;
    }

    Content content = operation.getRequestBody().getContent();
    // Try to get JSON schema
    MediaType jsonMediaType = content.get("application/json");
    if (jsonMediaType == null) {
      jsonMediaType = content.values().iterator().next();
    }

    if (jsonMediaType != null && jsonMediaType.getSchema() != null) {
      return serializeSchema(jsonMediaType.getSchema());
    }

    return null;
  }

  /** Extract response schema from operation (first successful response). */
  private String extractResponseSchema(Operation operation) {
    if (operation.getResponses() == null) {
      return null;
    }

    // Look for 200, 201, or first 2xx response
    ApiResponse response = null;
    if (operation.getResponses().get("200") != null) {
      response = operation.getResponses().get("200");
    } else if (operation.getResponses().get("201") != null) {
      response = operation.getResponses().get("201");
    } else {
      // Find first 2xx response
      for (Map.Entry<String, ApiResponse> entry : operation.getResponses().entrySet()) {
        if (entry.getKey().startsWith("2")) {
          response = entry.getValue();
          break;
        }
      }
    }

    if (response == null || response.getContent() == null) {
      return null;
    }

    Content content = response.getContent();
    MediaType jsonMediaType = content.get("application/json");
    if (jsonMediaType == null) {
      jsonMediaType = content.values().iterator().next();
    }

    if (jsonMediaType != null && jsonMediaType.getSchema() != null) {
      return serializeSchema(jsonMediaType.getSchema());
    }

    return null;
  }

  /** Extract examples from operation. */
  private List<ParsedExample> extractExamples(Operation operation) {
    List<ParsedExample> examples = new ArrayList<>();

    // Extract from request body examples
    if (operation.getRequestBody() != null && operation.getRequestBody().getContent() != null) {
      Content content = operation.getRequestBody().getContent();
      for (MediaType mediaType : content.values()) {
        if (mediaType.getExamples() != null) {
          for (Map.Entry<String, Example> exampleEntry : mediaType.getExamples().entrySet()) {
            Example example = exampleEntry.getValue();
            String requestBody = serializeExampleValue(example.getValue());
            examples.add(
                new ParsedExample(
                    exampleEntry.getKey(),
                    example.getSummary(),
                    example.getDescription(),
                    requestBody,
                    null,
                    null));
          }
        }
      }
    }

    // Extract from response examples
    if (operation.getResponses() != null) {
      for (Map.Entry<String, ApiResponse> responseEntry : operation.getResponses().entrySet()) {
        String statusCode = responseEntry.getKey();
        ApiResponse response = responseEntry.getValue();

        if (response.getContent() != null) {
          for (MediaType mediaType : response.getContent().values()) {
            if (mediaType.getExamples() != null) {
              for (Map.Entry<String, Example> exampleEntry : mediaType.getExamples().entrySet()) {
                Example example = exampleEntry.getValue();
                String responseBody = serializeExampleValue(example.getValue());
                examples.add(
                    new ParsedExample(
                        exampleEntry.getKey(),
                        example.getSummary(),
                        example.getDescription(),
                        null,
                        responseBody,
                        statusCode));
              }
            }
          }
        }
      }
    }

    return examples;
  }

  /** Determine operation category based on HTTP method and operation details. */
  private String determineCategory(String method, Operation operation) {
    String upperMethod = method.toUpperCase();
    switch (upperMethod) {
      case "GET":
        return "Retrieve";
      case "POST":
        // Check if it's a compute operation (queries, aggregations)
        String summary = operation.getSummary() != null ? operation.getSummary().toLowerCase() : "";
        String description =
            operation.getDescription() != null ? operation.getDescription().toLowerCase() : "";
        if (summary.contains("query")
            || summary.contains("aggregate")
            || summary.contains("compute")
            || summary.contains("calculate")
            || description.contains("query")
            || description.contains("aggregate")
            || description.contains("compute")
            || description.contains("calculate")) {
          return "Compute";
        }
        return "Create";
      case "PUT":
      case "PATCH":
        return "Update";
      case "DELETE":
        return "Delete";
      default:
        return "Retrieve";
    }
  }

  /**
   * Extract all tags from the OpenAPI spec.
   *
   * @return list of parsed tags
   */
  public List<ParsedTag> extractTags() {
    List<ParsedTag> tags = new ArrayList<>();

    if (openAPI.getTags() == null) {
      return tags;
    }

    for (Tag tag : openAPI.getTags()) {
      String description = tag.getDescription() != null ? tag.getDescription() : "";
      Map<String, String> externalDocs = null;
      if (tag.getExternalDocs() != null) {
        externalDocs = new HashMap<>();
        if (tag.getExternalDocs().getUrl() != null) {
          externalDocs.put("url", tag.getExternalDocs().getUrl());
        }
        if (tag.getExternalDocs().getDescription() != null) {
          externalDocs.put("description", tag.getExternalDocs().getDescription());
        }
      }
      tags.add(new ParsedTag(tag.getName(), description, externalDocs));
    }

    return tags;
  }

  /**
   * Extract all schema components from the OpenAPI spec.
   *
   * @return list of parsed schemas
   */
  public List<ParsedSchema> extractSchemas() {
    List<ParsedSchema> schemas = new ArrayList<>();

    if (openAPI.getComponents() == null || openAPI.getComponents().getSchemas() == null) {
      return schemas;
    }

    for (Map.Entry<String, Schema> schemaEntry : openAPI.getComponents().getSchemas().entrySet()) {
      String name = schemaEntry.getKey();
      Schema<?> schema = schemaEntry.getValue();
      String description = schema.getDescription() != null ? schema.getDescription() : "";
      schemas.add(new ParsedSchema(name, schema, description));
    }

    return schemas;
  }

  /** Get the OpenAPI info section. */
  public Map<String, Object> getInfo() {
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
    return info;
  }

  /** Serialize a schema to JSON string. */
  private String serializeSchema(Schema<?> schema) {
    try {
      // Handle schema references ($ref)
      if (schema.get$ref() != null) {
        Map<String, Object> refMap = new HashMap<>();
        refMap.put("$ref", schema.get$ref());
        // Try to resolve the reference if it's a component schema
        String refPath = schema.get$ref();
        if (refPath.startsWith("#/components/schemas/")) {
          String schemaName = refPath.substring("#/components/schemas/".length());
          Schema<?> resolvedSchema = resolveSchemaReference(schemaName);
          if (resolvedSchema != null) {
            // Include resolved schema properties
            Map<String, Object> resolvedMap = serializeSchemaToMap(resolvedSchema);
            refMap.putAll(resolvedMap);
          }
        }
        return jsonMapper.writeValueAsString(refMap);
      }

      // Convert schema to a simplified representation
      Map<String, Object> schemaMap = serializeSchemaToMap(schema);
      return jsonMapper.writeValueAsString(schemaMap);
    } catch (Exception e) {
      log.warn("Failed to serialize schema", e);
      return null;
    }
  }

  /** Serialize a schema to a map representation. */
  private Map<String, Object> serializeSchemaToMap(Schema<?> schema) {
    Map<String, Object> schemaMap = new HashMap<>();
    if (schema.getType() != null) {
      schemaMap.put("type", schema.getType());
    }
    if (schema.getProperties() != null) {
      // Serialize properties recursively
      Map<String, Object> propertiesMap = new HashMap<>();
      for (Map.Entry<String, Schema> propEntry : schema.getProperties().entrySet()) {
        Map<String, Object> propMap = serializeSchemaToMap(propEntry.getValue());
        propertiesMap.put(propEntry.getKey(), propMap);
      }
      schemaMap.put("properties", propertiesMap);
    }
    if (schema.getRequired() != null) {
      schemaMap.put("required", schema.getRequired());
    }
    if (schema.getDescription() != null) {
      schemaMap.put("description", schema.getDescription());
    }
    if (schema.getItems() != null) {
      schemaMap.put("items", serializeSchemaToMap(schema.getItems()));
    }
    return schemaMap;
  }

  /** Resolve a schema reference from components. */
  private Schema<?> resolveSchemaReference(String schemaName) {
    if (openAPI.getComponents() == null || openAPI.getComponents().getSchemas() == null) {
      return null;
    }
    return openAPI.getComponents().getSchemas().get(schemaName);
  }

  /** Serialize an example value to JSON string. */
  private String serializeExampleValue(Object value) {
    if (value == null) {
      return null;
    }
    try {
      if (value instanceof String) {
        return (String) value;
      }
      return jsonMapper.writeValueAsString(value);
    } catch (Exception e) {
      log.warn("Failed to serialize example value", e);
      return value.toString();
    }
  }

  /** Get a summary of the OpenAPI spec (info, number of operations, tags, etc.). */
  public Map<String, Object> getSpecSummary() {
    Map<String, Object> summary = new HashMap<>();
    summary.put("info", getInfo());
    summary.put("operationCount", extractOperations().size());
    summary.put("tagCount", extractTags().size());
    summary.put("schemaCount", extractSchemas().size());
    return summary;
  }
}
