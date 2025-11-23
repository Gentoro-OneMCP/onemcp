package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.indexing.driver.GraphQueryDriver;
import com.gentoro.onemcp.indexing.driver.arangodb.ArangoQueryDriver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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

  /** Query context item representing an entity with operations to retrieve. */
  public static class ContextItem {
    private String entity;
    private List<String> operations;
    private Integer confidence;
    private String referral;

    public ContextItem() {}

    public ContextItem(
        String entity, List<String> operations, Integer confidence, String referral) {
      this.entity = entity;
      this.operations = operations;
      this.confidence = confidence;
      this.referral = referral;
    }

    public String getEntity() {
      return entity;
    }

    public void setEntity(String entity) {
      this.entity = entity;
    }

    public List<String> getOperations() {
      return operations;
    }

    public void setOperations(List<String> operations) {
      this.operations = operations;
    }

    public Integer getConfidence() {
      return confidence;
    }

    public void setConfidence(Integer confidence) {
      this.confidence = confidence;
    }

    public String getReferral() {
      return referral;
    }

    public void setReferral(String referral) {
      this.referral = referral;
    }
  }

  /** Query request containing context items. */
  public static class QueryRequest {
    private List<ContextItem> context;

    public QueryRequest() {}

    public QueryRequest(List<ContextItem> context) {
      this.context = context;
    }

    public List<ContextItem> getContext() {
      return context;
    }

    public void setContext(List<ContextItem> context) {
      this.context = context;
    }
  }

  /** Query result containing all retrieved information for each context item. */
  public static class QueryResult {
    private String entity;
    private List<String> requestedOperations;
    private Integer confidence;
    private String referral;
    private Map<String, Object> entityInfo;
    private List<Map<String, Object>> fields;
    private List<OperationResult> operations;

    public QueryResult() {}

    public String getEntity() {
      return entity;
    }

    public void setEntity(String entity) {
      this.entity = entity;
    }

    public List<String> getRequestedOperations() {
      return requestedOperations;
    }

    public void setRequestedOperations(List<String> requestedOperations) {
      this.requestedOperations = requestedOperations;
    }

    public Integer getConfidence() {
      return confidence;
    }

    public void setConfidence(Integer confidence) {
      this.confidence = confidence;
    }

    public String getReferral() {
      return referral;
    }

    public void setReferral(String referral) {
      this.referral = referral;
    }

    public Map<String, Object> getEntityInfo() {
      return entityInfo;
    }

    public void setEntityInfo(Map<String, Object> entityInfo) {
      this.entityInfo = entityInfo;
    }

    public List<Map<String, Object>> getFields() {
      return fields;
    }

    public void setFields(List<Map<String, Object>> fields) {
      this.fields = fields;
    }

    public List<OperationResult> getOperations() {
      return operations;
    }

    public void setOperations(List<OperationResult> operations) {
      this.operations = operations;
    }
  }

  /** Operation result containing complete operation information. */
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

    public String getOperationId() {
      return operationId;
    }

    public void setOperationId(String operationId) {
      this.operationId = operationId;
    }

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public String getSummary() {
      return summary;
    }

    public void setSummary(String summary) {
      this.summary = summary;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public String getCategory() {
      return category;
    }

    public void setCategory(String category) {
      this.category = category;
    }

    public String getSignature() {
      return signature;
    }

    public void setSignature(String signature) {
      this.signature = signature;
    }

    public String getRequestSchema() {
      return requestSchema;
    }

    public void setRequestSchema(String requestSchema) {
      this.requestSchema = requestSchema;
    }

    public String getResponseSchema() {
      return responseSchema;
    }

    public void setResponseSchema(String responseSchema) {
      this.responseSchema = responseSchema;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> tags) {
      this.tags = tags;
    }

    public List<Map<String, Object>> getExamples() {
      return examples;
    }

    public void setExamples(List<Map<String, Object>> examples) {
      this.examples = examples;
    }

    public List<Map<String, Object>> getDocumentation() {
      return documentation;
    }

    public void setDocumentation(List<Map<String, Object>> documentation) {
      this.documentation = documentation;
    }
  }

  /**
   * Execute an optimized graph query to retrieve all relevant information for the given context.
   *
   * <p>This method uses a single optimized AQL query to traverse the graph and retrieve:
   *
   * <ul>
   *   <li>Entity information
   *   <li>Entity fields
   *   <li>Operations matching the requested categories
   *   <li>Examples for each operation
   *   <li>Documentation for operations and entities
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
    result.setOperations(Collections.emptyList());
    return result;
  }

  /** Shutdown the underlying driver and release resources. */
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

  /**
   * @return configured driver name (lowercase).
   */
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
