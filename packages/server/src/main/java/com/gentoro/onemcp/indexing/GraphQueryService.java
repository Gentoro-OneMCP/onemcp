package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.indexing.driver.GraphQueryDriver;
import com.gentoro.onemcp.indexing.driver.arangodb.ArangoQueryDriver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

/**
 * Service for querying the knowledge graph independent of the underlying driver implementation.
 *
 * <p>This service orchestrates optimized traversals for retrieving entity details, operations,
 * examples, and documentation using the configured {@link GraphQueryDriver}.
 */
public class GraphQueryService implements AutoCloseable {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(GraphQueryService.class);

  private final OneMcp oneMcp;
  private final String driverName;
  private final String handbookName;
  private final GraphQueryDriver queryDriver;
  private boolean driverInitialized;

  /**
   * Create a new graph query service using the provided OneMcp context and configured driver.
   *
   * @param oneMcp the OneMcp application context
   */
  public GraphQueryService(OneMcp oneMcp) {
    this(oneMcp, null);
  }

  /**
   * Create a new graph query service with an explicit driver override.
   *
   * @param oneMcp the application context
   * @param customDriver optional custom driver (when {@code null}, driver determined via config)
   */
  public GraphQueryService(OneMcp oneMcp, GraphQueryDriver customDriver) {
    this.oneMcp = Objects.requireNonNull(oneMcp, "oneMcp must not be null");
    this.handbookName = oneMcp.knowledgeBase().getHandbookName();
    this.driverName = resolveDriverName(oneMcp);
    this.queryDriver = customDriver != null ? customDriver : createDriver(this.driverName);
    this.driverInitialized = queryDriver != null && queryDriver.isInitialized();
  }

  /**
   * Create a query service from an already constructed driver (useful for tests).
   *
   * @param queryDriver the driver to use
   */
  public GraphQueryService(GraphQueryDriver queryDriver) {
    this.oneMcp = null;
    this.handbookName = queryDriver != null ? queryDriver.getHandbookName() : null;
    this.driverName = queryDriver != null ? queryDriver.getDriverName() : "unknown";
    this.queryDriver = queryDriver;
    this.driverInitialized = queryDriver != null && queryDriver.isInitialized();
  }

  /**
   * Query context item representing an entity with operations to retrieve.
   */
  public static class ContextItem {
    private String entity;
    private List<String> operations;
    private Integer confidence;
    private String referral;

    public ContextItem() {}

    public ContextItem(String entity, List<String> operations, Integer confidence, String referral) {
      this.entity = entity;
      this.operations = operations;
      this.confidence = confidence;
      this.referral = referral;
    }

    public String getEntity() { return entity; }
    public void setEntity(String entity) { this.entity = entity; }
    public List<String> getOperations() { return operations; }
    public void setOperations(List<String> operations) { this.operations = operations; }
    public Integer getConfidence() { return confidence; }
    public void setConfidence(Integer confidence) { this.confidence = confidence; }
    public String getReferral() { return referral; }
    public void setReferral(String referral) { this.referral = referral; }
  }

  /**
   * Query request containing context items.
   */
  public static class QueryRequest {
    private List<ContextItem> context;

    public QueryRequest() {}

    public QueryRequest(List<ContextItem> context) {
      this.context = context;
    }

    public List<ContextItem> getContext() { return context; }
    public void setContext(List<ContextItem> context) { this.context = context; }
  }

  /**
   * Query result containing all retrieved information for each context item.
   */
  public static class QueryResult {
    private String entity;
    private List<String> requestedOperations;
    private Integer confidence;
    private String referral;
    private Map<String, Object> entityInfo;
    private List<Map<String, Object>> fields;
    private List<Map<String, Object>> entityDocumentation;
    private List<OperationResult> operations;

    public QueryResult() {}

    public String getEntity() { return entity; }
    public void setEntity(String entity) { this.entity = entity; }
    public List<String> getRequestedOperations() { return requestedOperations; }
    public void setRequestedOperations(List<String> requestedOperations) { 
      this.requestedOperations = requestedOperations; 
    }
    public Integer getConfidence() { return confidence; }
    public void setConfidence(Integer confidence) { this.confidence = confidence; }
    public String getReferral() { return referral; }
    public void setReferral(String referral) { this.referral = referral; }
    public Map<String, Object> getEntityInfo() { return entityInfo; }
    public void setEntityInfo(Map<String, Object> entityInfo) { this.entityInfo = entityInfo; }
    public List<Map<String, Object>> getFields() { return fields; }
    public void setFields(List<Map<String, Object>> fields) { this.fields = fields; }
    public List<Map<String, Object>> getEntityDocumentation() { return entityDocumentation; }
    public void setEntityDocumentation(List<Map<String, Object>> entityDocumentation) { 
      this.entityDocumentation = entityDocumentation; 
    }
    public List<OperationResult> getOperations() { return operations; }
    public void setOperations(List<OperationResult> operations) { this.operations = operations; }
  }

  /**
   * Operation result containing complete operation information.
   */
  public static class OperationResult {
    private String operationId;
    private String method;
    private String path;
    private String summary;
    private String description;
    private String category;
    private String signature;
    private String requestSchema;
    private String responseSchema;
    private List<String> tags;
    private List<Map<String, Object>> examples;
    private List<Map<String, Object>> documentation;

    public OperationResult() {}

    public String getOperationId() { return operationId; }
    public void setOperationId(String operationId) { this.operationId = operationId; }
    public String getMethod() { return method; }
    public void setMethod(String method) { this.method = method; }
    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }
    public String getSummary() { return summary; }
    public void setSummary(String summary) { this.summary = summary; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public String getSignature() { return signature; }
    public void setSignature(String signature) { this.signature = signature; }
    public String getRequestSchema() { return requestSchema; }
    public void setRequestSchema(String requestSchema) { this.requestSchema = requestSchema; }
    public String getResponseSchema() { return responseSchema; }
    public void setResponseSchema(String responseSchema) { this.responseSchema = responseSchema; }
    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }
    public List<Map<String, Object>> getExamples() { return examples; }
    public void setExamples(List<Map<String, Object>> examples) { this.examples = examples; }
    public List<Map<String, Object>> getDocumentation() { return documentation; }
    public void setDocumentation(List<Map<String, Object>> documentation) { 
      this.documentation = documentation; 
    }
  }

  /**
   * Flattened query result item with kind, content, and reference.
   */
  public static class FlattenedResult {
    private String kind;
    private String content;
    private String ref;

    public FlattenedResult() {}

    public FlattenedResult(String kind, String content, String ref) {
      this.kind = kind;
      this.content = content;
      this.ref = ref;
    }

    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public String getRef() { return ref; }
    public void setRef(String ref) { this.ref = ref; }
  }

  /**
   * Flattened results grouped by entity.
   */
  public static class FlattenedResultGroup {
    private String entity;
    private List<FlattenedResult> items;

    public FlattenedResultGroup() {}

    public FlattenedResultGroup(String entity, List<FlattenedResult> items) {
      this.entity = entity;
      this.items = items;
    }

    public String getEntity() { return entity; }
    public void setEntity(String entity) { this.entity = entity; }
    public List<FlattenedResult> getItems() { return items; }
    public void setItems(List<FlattenedResult> items) { this.items = items; }
  }

  /**
   * Operation-oriented result item with kind, content, and reference.
   */
  public static class OperationOrientedItem {
    private String kind;
    private String content;
    private String ref;

    public OperationOrientedItem() {}

    public OperationOrientedItem(String kind, String content, String ref) {
      this.kind = kind;
      this.content = content;
      this.ref = ref;
    }

    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public String getRef() { return ref; }
    public void setRef(String ref) { this.ref = ref; }
  }

  /**
   * Operation-oriented result grouped by operation.
   */
  public static class OperationOrientedGroup {
    private String operation;
    private List<OperationOrientedItem> items;

    public OperationOrientedGroup() {}

    public OperationOrientedGroup(String operation, List<OperationOrientedItem> items) {
      this.operation = operation;
      this.items = items;
    }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public List<OperationOrientedItem> getItems() { return items; }
    public void setItems(List<OperationOrientedItem> items) { this.items = items; }
  }

  /**
   * Execute an optimized graph query to retrieve all relevant information for the given context.
   *
   * <p>This method uses a single optimized AQL query to traverse the graph and retrieve:
   * <ul>
   *   <li>Entity information</li>
   *   <li>Entity fields</li>
   *   <li>Operations matching the requested categories</li>
   *   <li>Examples for each operation</li>
   *   <li>Documentation for operations and entities</li>
   * </ul>
   *
   * @param request the query request containing context items
   * @return list of query results, one per context item
   */
  public List<QueryResult> query(QueryRequest request) {
    if (request == null || request.getContext() == null || request.getContext().isEmpty()) {
      log.warn("Empty query request received");
      return Collections.emptyList();
    }

    ensureDriverReady();

    log.info("Processing graph query with {} context items", request.getContext().size());

    List<QueryResult> results = new ArrayList<>();

    for (ContextItem contextItem : request.getContext()) {
      try {
        QueryResult result = queryContextItem(contextItem);
        results.add(result);
      } catch (Exception e) {
        log.error("Failed to query context item: {}", contextItem.getEntity(), e);
        // Add empty result for this context item to maintain order
        QueryResult errorResult = new QueryResult();
        errorResult.setEntity(contextItem.getEntity());
        errorResult.setRequestedOperations(contextItem.getOperations());
        errorResult.setConfidence(contextItem.getConfidence());
        errorResult.setReferral(contextItem.getReferral());
        errorResult.setOperations(Collections.emptyList());
        results.add(errorResult);
      }
    }

    return results;
  }

  /**
   * Execute a graph query and return flattened results grouped by entity.
   *
   * <p>This method executes the same query as {@link #query(QueryRequest)} but returns results
   * in a flattened format with kind, content, and reference fields, grouped by entity.
   *
   * @param request the query request containing context items
   * @return list of flattened result groups, one per entity
   */
  public List<FlattenedResultGroup> queryFlattened(QueryRequest request) {
    List<QueryResult> results = query(request);
    return flattenResults(results);
  }

  /**
   * Execute a graph query and return operation-oriented results grouped by operation.
   *
   * <p>This method returns results focused on operations, grouped by operation name.
   * Each group contains items with:
   * <ul>
   *   <li>kind: "doc", "signature", or "example"</li>
   *   <li>content: markdown-formatted content</li>
   *   <li>ref: reference URL or source file</li>
   * </ul>
   *
   * @param request the query request containing context items
   * @return list of operation-oriented groups
   */
  public List<OperationOrientedGroup> queryOperationOriented(QueryRequest request) {
    List<QueryResult> results = query(request);
    return flattenToOperationOriented(results);
  }

  /**
   * Flatten query results into operation-oriented groups.
   *
   * <p>Items are grouped by operation, with only doc, signature, and example kinds.
   *
   * @param results the query results to flatten
   * @return list of operation-oriented groups
   */
  @SuppressWarnings("unchecked")
  public List<OperationOrientedGroup> flattenToOperationOriented(List<QueryResult> results) {
    Map<String, List<OperationOrientedItem>> operationGroups = new HashMap<>();
    Set<String> seenDocKeys = new HashSet<>();

    for (QueryResult result : results) {
      if (result.getOperations() == null) {
        continue;
      }

      // Process each operation
      for (OperationResult op : result.getOperations()) {
        String operationName = op.getOperationId();
        if (operationName == null || operationName.isBlank()) {
          operationName = buildOperationName(op.getMethod(), op.getPath());
        }

        // Get or create group for this operation
        List<OperationOrientedItem> groupItems = operationGroups.computeIfAbsent(operationName, k -> new ArrayList<>());

        // Add operation signature
        if (op.getSignature() != null && !op.getSignature().isBlank()) {
          String ref = buildOperationRef(op.getMethod(), op.getPath());
          groupItems.add(new OperationOrientedItem("signature", op.getSignature(), ref));
        }

        // Add examples
        if (op.getExamples() != null) {
          for (Map<String, Object> example : op.getExamples()) {
            String ref = buildOperationRef(op.getMethod(), op.getPath());

            // Build example content from request/response
            StringBuilder exampleContent = new StringBuilder();
            String name = (String) example.get("name");
            if (name != null && !name.isBlank()) {
              exampleContent.append("**").append(name).append("**\n\n");
            }

            String description = (String) example.get("description");
            if (description != null && !description.isBlank()) {
              exampleContent.append(description).append("\n\n");
            }

            String requestBody = (String) example.get("requestBody");
            if (requestBody != null && !requestBody.isBlank()) {
              exampleContent.append("**Request:**\n```json\n").append(requestBody).append("\n```\n\n");
            }

            String responseBody = (String) example.get("responseBody");
            if (responseBody != null && !responseBody.isBlank()) {
              exampleContent.append("**Response:**\n```json\n").append(responseBody).append("\n```\n");
            }

            if (exampleContent.length() > 0) {
              groupItems.add(new OperationOrientedItem("example", exampleContent.toString(), ref));
            }
          }
        }

        // Add operation documentation (deduplicated by key)
        if (op.getDocumentation() != null) {
          for (Map<String, Object> doc : op.getDocumentation()) {
            String docKey = (String) doc.get("_key");
            if (docKey == null) {
              docKey = (String) doc.get("key");
            }

            // Skip if we've already seen this documentation node
            if (docKey != null && seenDocKeys.contains(docKey)) {
              continue;
            }

            String content = (String) doc.get("content");
            if (content != null && !content.isBlank()) {
              if (docKey != null) {
                seenDocKeys.add(docKey);
              }
              String ref = (String) doc.get("sourceFile");
              if (ref == null || ref.isBlank()) {
                ref = buildOperationRef(op.getMethod(), op.getPath());
              }
              groupItems.add(new OperationOrientedItem("doc", content, ref));
            }
          }
        }
      }
    }

    // Convert to list of groups
    return operationGroups.entrySet().stream()
        .map(entry -> new OperationOrientedGroup(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * Build an operation name from method and path.
   */
  private String buildOperationName(String method, String path) {
    if (method != null && !method.isBlank() && path != null && !path.isBlank()) {
      return method.toUpperCase() + " " + path;
    }
    if (path != null && !path.isBlank()) {
      return path;
    }
    if (method != null && !method.isBlank()) {
      return method.toUpperCase();
    }
    return "unknown";
  }

  /**
   * Flatten query results into groups by entity, with each group containing items with kind, content, and reference.
   *
   * @param results the query results to flatten
   * @return list of flattened result groups, one per entity
   */
  @SuppressWarnings("unchecked")
  public List<FlattenedResultGroup> flattenResults(List<QueryResult> results) {
    List<FlattenedResultGroup> groups = new ArrayList<>();
    // Track documentation nodes by key to avoid duplicates
    Set<String> seenDocKeys = new HashSet<>();

    for (QueryResult result : results) {
      List<FlattenedResult> flattened = new ArrayList<>();
      String entityName = result.getEntity();
      if (entityName == null) {
        entityName = "unknown";
      }

      // Add entity description
      if (result.getEntityInfo() != null) {
        String description = (String) result.getEntityInfo().get("description");
        if (description != null && !description.isBlank()) {
          String ref = buildEntityRef(entityName, result.getEntityInfo());
          flattened.add(new FlattenedResult("entity", description, ref));
        }
      }

      // Add entity documentation (deduplicated by key)
      if (result.getEntityDocumentation() != null) {
        Map<String, Object> entityInfo = result.getEntityInfo();
        for (Map<String, Object> doc : result.getEntityDocumentation()) {
          String docKey = (String) doc.get("_key");
          if (docKey == null) {
            docKey = (String) doc.get("key");
          }
          
          // Skip if we've already seen this documentation node
          if (docKey != null && seenDocKeys.contains(docKey)) {
            continue;
          }
          
          String content = (String) doc.get("content");
          if (content != null && !content.isBlank()) {
            if (docKey != null) {
              seenDocKeys.add(docKey);
            }
            String ref = (String) doc.get("sourceFile");
            if (ref == null || ref.isBlank()) {
              ref = entityInfo != null ? buildEntityRef(entityName, entityInfo) : String.format("/entities/%s", entityName);
            }
            flattened.add(new FlattenedResult("doc", content, ref));
          }
        }
      }

      // Add fields
      if (result.getFields() != null) {
        for (Map<String, Object> field : result.getFields()) {
          String description = (String) field.get("description");
          if (description != null && !description.isBlank()) {
            String fieldName = (String) field.get("name");
            String ref = buildFieldRef(entityName, fieldName);
            flattened.add(new FlattenedResult("field", description, ref));
          }
        }
      }

      // Add operations
      if (result.getOperations() != null) {
        for (OperationResult op : result.getOperations()) {
          // Add operation signature
          if (op.getSignature() != null && !op.getSignature().isBlank()) {
            String ref = buildOperationRef(op.getMethod(), op.getPath());
            flattened.add(new FlattenedResult("signature", op.getSignature(), ref));
          }

          // Note: Operation description and summary are NOT documentation nodes,
          // they are just fields from the operation. We only include actual documentation nodes.

          // Add examples
          if (op.getExamples() != null) {
            for (Map<String, Object> example : op.getExamples()) {
              String ref = buildOperationRef(op.getMethod(), op.getPath());
              
              // Build example content from request/response
              StringBuilder exampleContent = new StringBuilder();
              String name = (String) example.get("name");
              if (name != null && !name.isBlank()) {
                exampleContent.append("**").append(name).append("**\n\n");
              }
              
              String description = (String) example.get("description");
              if (description != null && !description.isBlank()) {
                exampleContent.append(description).append("\n\n");
              }
              
              String requestBody = (String) example.get("requestBody");
              if (requestBody != null && !requestBody.isBlank()) {
                exampleContent.append("**Request:**\n```json\n").append(requestBody).append("\n```\n\n");
              }
              
              String responseBody = (String) example.get("responseBody");
              if (responseBody != null && !responseBody.isBlank()) {
                exampleContent.append("**Response:**\n```json\n").append(responseBody).append("\n```\n");
              }
              
              if (exampleContent.length() > 0) {
                flattened.add(new FlattenedResult("example", exampleContent.toString(), ref));
              }
            }
          }

          // Add operation documentation (deduplicated by key)
          if (op.getDocumentation() != null) {
            for (Map<String, Object> doc : op.getDocumentation()) {
              String docKey = (String) doc.get("_key");
              if (docKey == null) {
                docKey = (String) doc.get("key");
              }
              
              // Skip if we've already seen this documentation node
              if (docKey != null && seenDocKeys.contains(docKey)) {
                continue;
              }
              
              String content = (String) doc.get("content");
              if (content != null && !content.isBlank()) {
                if (docKey != null) {
                  seenDocKeys.add(docKey);
                }
                String ref = (String) doc.get("sourceFile");
                if (ref == null || ref.isBlank()) {
                  ref = buildOperationRef(op.getMethod(), op.getPath());
                }
                flattened.add(new FlattenedResult("doc", content, ref));
              }
            }
          }
        }
      }

      // Create a group for this entity
      groups.add(new FlattenedResultGroup(entityName, flattened));
    }

    return groups;
  }

  /**
   * Build a reference URL for an entity.
   */
  private String buildEntityRef(String entityName, Map<String, Object> entityInfo) {
    String serviceSlug = (String) entityInfo.get("serviceSlug");
    if (serviceSlug != null && !serviceSlug.isBlank()) {
      return String.format("/%s/entities/%s", serviceSlug, entityName);
    }
    return String.format("/entities/%s", entityName);
  }

  /**
   * Build a reference URL for a field.
   */
  private String buildFieldRef(String entityName, String fieldName) {
    if (fieldName != null && !fieldName.isBlank()) {
      return String.format("/entities/%s/fields/%s", entityName, fieldName);
    }
    return String.format("/entities/%s", entityName);
  }

  /**
   * Build a reference URL for an operation.
   */
  private String buildOperationRef(String method, String path) {
    if (path != null && !path.isBlank()) {
      return path.startsWith("/") ? path : "/" + path;
    }
    if (method != null && !method.isBlank()) {
      return String.format("/%s", method.toLowerCase());
    }
    return "/";
  }

  /**
   * Query a single context item using an optimized AQL traversal.
   *
   * @param contextItem the context item to query
   * @return query result with all relevant information
   */
  private QueryResult queryContextItem(ContextItem contextItem) {
    String entityName = contextItem.getEntity();
    List<String> requestedCategories = contextItem.getOperations();

    log.debug("Querying entity '{}' for operations: {}", entityName, requestedCategories);

    try {
      Map<String, Object> rawResult = queryDriver.queryContext(entityName, requestedCategories);

      if (rawResult == null) {
        log.warn("No results found for entity: {}", entityName);
        return createEmptyResult(contextItem);
      }

      return parseQueryResult(rawResult, contextItem);

    } catch (Exception e) {
      log.error("Error executing graph query for entity: {}", entityName, e);
      throw new IoException("Graph query failed for entity: " + entityName, e);
    }
  }

  /**
   * Parse the raw query result into a structured QueryResult object.
   *
   * @param rawResult the raw result from ArangoDB
   * @param contextItem the original context item
   * @return structured query result
   */
  @SuppressWarnings("unchecked")
  private QueryResult parseQueryResult(Map<String, Object> rawResult, ContextItem contextItem) {
    QueryResult result = new QueryResult();
    
    result.setEntity(contextItem.getEntity());
    result.setRequestedOperations(contextItem.getOperations());
    result.setConfidence(contextItem.getConfidence());
    result.setReferral(contextItem.getReferral());

    // Parse entity info
    Map<String, Object> entityMap = (Map<String, Object>) rawResult.get("entity");
    if (entityMap != null) {
      // Remove internal ArangoDB fields
      entityMap = new HashMap<>(entityMap);
      entityMap.remove("_id");
      entityMap.remove("_rev");
      result.setEntityInfo(entityMap);
    }

    // Parse fields
    List<Map<String, Object>> fieldsList = (List<Map<String, Object>>) rawResult.get("fields");
    if (fieldsList != null) {
      result.setFields(
          fieldsList.stream().map(this::cleanInternalFields).collect(Collectors.toList()));
    } else {
      result.setFields(Collections.emptyList());
    }

    // Parse entity documentation
    List<Map<String, Object>> entityDocsList = (List<Map<String, Object>>) rawResult.get("entityDocumentation");
    if (entityDocsList != null) {
      result.setEntityDocumentation(
          entityDocsList.stream().map(this::cleanInternalFields).collect(Collectors.toList()));
    } else {
      result.setEntityDocumentation(Collections.emptyList());
    }

    // Parse operations
    List<Map<String, Object>> operationsList =
        (List<Map<String, Object>>) rawResult.get("operations");
    if (operationsList != null) {
      List<OperationResult> operations =
          operationsList.stream().map(this::parseOperation).collect(Collectors.toList());
      result.setOperations(operations);
    } else {
      result.setOperations(Collections.emptyList());
    }

    return result;
  }

  /**
   * Parse an operation map into an OperationResult object.
   *
   * @param opMap the operation map from query result
   * @return structured operation result
   */
  @SuppressWarnings("unchecked")
  private OperationResult parseOperation(Map<String, Object> opMap) {
    OperationResult op = new OperationResult();
    
    op.setOperationId((String) opMap.get("operationId"));
    op.setMethod((String) opMap.get("method"));
    op.setPath((String) opMap.get("path"));
    op.setSummary((String) opMap.get("summary"));
    op.setDescription((String) opMap.get("description"));
    op.setCategory((String) opMap.get("category"));
    op.setSignature((String) opMap.get("signature"));
    op.setRequestSchema((String) opMap.get("requestSchema"));
    op.setResponseSchema((String) opMap.get("responseSchema"));
    op.setTags((List<String>) opMap.get("tags"));

    // Parse examples
    List<Map<String, Object>> examplesList = (List<Map<String, Object>>) opMap.get("examples");
    if (examplesList != null) {
      op.setExamples(
          examplesList.stream().map(this::cleanInternalFields).collect(Collectors.toList()));
    } else {
      op.setExamples(Collections.emptyList());
    }

    // Parse documentation
    List<Map<String, Object>> docsList = (List<Map<String, Object>>) opMap.get("documentation");
    if (docsList != null) {
      op.setDocumentation(
          docsList.stream().map(this::cleanInternalFields).collect(Collectors.toList()));
    } else {
      op.setDocumentation(Collections.emptyList());
    }

    return op;
  }

  /**
   * Remove internal driver-specific fields from a map (e.g., ArangoDB metadata).
   *
   * @param map the map to clean
   * @return cleaned map
   */
  private Map<String, Object> cleanInternalFields(Map<String, Object> map) {
    Map<String, Object> cleaned = new HashMap<>(map);
    cleaned.remove("_id");
    cleaned.remove("_rev");
    cleaned.remove("_key");
    return cleaned;
  }

  /**
   * Create an empty result for a context item when no data is found.
   *
   * @param contextItem the context item
   * @return empty query result
   */
  private QueryResult createEmptyResult(ContextItem contextItem) {
    QueryResult result = new QueryResult();
    result.setEntity(contextItem.getEntity());
    result.setRequestedOperations(contextItem.getOperations());
    result.setConfidence(contextItem.getConfidence());
    result.setReferral(contextItem.getReferral());
    result.setFields(Collections.emptyList());
    result.setEntityDocumentation(Collections.emptyList());
    result.setOperations(Collections.emptyList());
    return result;
  }

  /**
   * Shutdown the underlying driver and release resources.
   */
  public void shutdown() {
    if (queryDriver != null) {
      queryDriver.shutdown();
      driverInitialized = false;
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  /** @return configured driver name (lowercase). */
  public String getDriverName() {
    return driverName;
  }

  private void ensureDriverReady() {
    if (queryDriver == null) {
      throw new IoException(
          "Graph query driver '%s' is not available or not configured".formatted(driverName));
    }

    if (!driverInitialized) {
      queryDriver.initialize();
      driverInitialized = queryDriver.isInitialized();
    }

    if (!driverInitialized) {
      throw new IoException("Failed to initialize graph query driver: " + driverName);
    }
  }

  private GraphQueryDriver createDriver(String name) {
    if (name == null) {
      log.warn("Graph query driver name is null; queries are disabled");
      return null;
    }

    return switch (name) {
      case "arangodb" -> new ArangoQueryDriver(oneMcp, handbookName);
      default -> {
        log.warn("Graph query driver '{}' not supported yet; queries are disabled", name);
        yield null;
      }
    };
  }

  private String resolveDriverName(OneMcp oneMcp) {
    Configuration config = oneMcp.configuration();
    String queryKey = "graph.query.driver";
    String indexingKey = "graph.indexing.driver";

    String driver =
        config.containsKey(queryKey)
            ? config.getString(queryKey, "arangodb")
            : config.getString(indexingKey, "arangodb");

    return driver != null ? driver.toLowerCase(Locale.ROOT) : "arangodb";
  }
}
