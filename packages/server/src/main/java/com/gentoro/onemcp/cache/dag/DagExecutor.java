package com.gentoro.onemcp.cache.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.cache.dag.conversion.EnumMapper;
import com.gentoro.onemcp.cache.dag.conversion.ValueConverter;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executes DAG plans.
 * 
 * <p>Supports all 10 node types:
 * - ConvertValue, MapEnum, CustomLogic (value transformation)
 * - Filter, Project, Sort, Aggregate, Join (structural query)
 * - ApiCall, LimitOffset (execution)
 */
public class DagExecutor {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(DagExecutor.class);

  private final ValueConverter valueConverter;
  private final EnumMapper enumMapper;
  private final OperationRegistry operationRegistry;
  private final CustomLogicExecutor customLogicExecutor;

  public DagExecutor(OperationRegistry operationRegistry) {
    this.operationRegistry = operationRegistry;
    this.valueConverter = new ValueConverter();
    this.enumMapper = new EnumMapper();
    this.customLogicExecutor = new CustomLogicExecutor();
  }

  /**
   * Execute a DAG plan.
   * 
   * @param plan the DAG plan
   * @param initialValues initial PS values (conceptual DTN strings)
   * @return final execution result
   */
  public JsonNode execute(DagPlan plan, Map<String, String> initialValues) {
    Map<String, DagNode> nodeMap = plan.getNodeMap();
    Map<String, JsonNode> nodeResults = new HashMap<>();
    
    // Add initial values to results
    if (initialValues != null) {
      ObjectNode initialNode = JacksonUtility.getJsonMapper().createObjectNode();
      for (Map.Entry<String, String> entry : initialValues.entrySet()) {
        initialNode.put(entry.getKey(), entry.getValue());
      }
      nodeResults.put("_initial", initialNode);
    }

    // Topological sort to determine execution order
    List<String> executionOrder = topologicalSort(plan.getNodes(), nodeMap);

    // Execute nodes in dependency order
    log.debug("Execution order: {}", executionOrder);
    for (String nodeId : executionOrder) {
      DagNode node = nodeMap.get(nodeId);
      if (node == null) {
        throw new DagExecutionException("Node not found: " + nodeId);
      }

      try {
        log.debug("Executing node: {} (type: {})", nodeId, node.getType());
        JsonNode result = executeNode(node, nodeResults, nodeMap);
        nodeResults.put(nodeId, result);
        try {
          log.debug("Node {} result: {}", nodeId, JacksonUtility.getJsonMapper().writeValueAsString(result));
        } catch (Exception logEx) {
          log.debug("Node {} result: (could not serialize)", nodeId);
        }
      } catch (Exception e) {
        throw new DagExecutionException("Failed to execute node " + nodeId + ": " + e.getMessage(), e);
      }
    }

    // Return result from last node (or entry point)
    String finalNodeId = plan.getEntryPoint();
    if (finalNodeId != null && nodeResults.containsKey(finalNodeId)) {
      return nodeResults.get(finalNodeId);
    }
    
    // Fallback: return last executed node result
    if (!executionOrder.isEmpty()) {
      String lastNodeId = executionOrder.get(executionOrder.size() - 1);
      return nodeResults.get(lastNodeId);
    }

    return JacksonUtility.getJsonMapper().createObjectNode();
  }

  private JsonNode executeNode(DagNode node, Map<String, JsonNode> nodeResults, 
      Map<String, DagNode> nodeMap) {
    String type = node.getType();
    ObjectNode config = node.getConfig();

    switch (type) {
      case "ConvertValue":
        return executeConvertValue(node, config, nodeResults);
      case "MapEnum":
        return executeMapEnum(node, config, nodeResults);
      case "CustomLogic":
        return executeCustomLogic(node, config, nodeResults);
      case "ApiCall":
        return executeApiCall(node, config, nodeResults);
      case "Filter":
        return executeFilter(node, config, nodeResults);
      case "Project":
        return executeProject(node, config, nodeResults);
      case "Sort":
        return executeSort(node, config, nodeResults);
      case "Aggregate":
        return executeAggregate(node, config, nodeResults);
      case "Join":
        return executeJoin(node, config, nodeResults);
      case "LimitOffset":
        return executeLimitOffset(node, config, nodeResults);
      default:
        throw new DagExecutionException("Unknown node type: " + type);
    }
  }

  private JsonNode executeConvertValue(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    String conceptualFieldKind = config.has("conceptualFieldKind") 
        ? config.get("conceptualFieldKind").asText() : null;
    String targetFormat = config.has("targetFormat") 
        ? config.get("targetFormat").asText() : null;
    String value = config.has("value") ? config.get("value").asText() : null;

    // Value might reference another node output
    if (value != null && value.startsWith("$.")) {
      value = resolveJsonPath(value, nodeResults);
    }

    Object converted = valueConverter.convert(conceptualFieldKind, targetFormat, value);
    
    ObjectNode result = JacksonUtility.getJsonMapper().createObjectNode();
    if (converted instanceof String) {
      result.put("value", (String) converted);
    } else if (converted instanceof Number) {
      result.set("value", JacksonUtility.getJsonMapper().valueToTree(converted));
    } else if (converted instanceof Boolean) {
      result.put("value", (Boolean) converted);
    } else {
      result.put("value", String.valueOf(converted));
    }
    return result;
  }

  private JsonNode executeMapEnum(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    String conceptualFieldKind = config.has("conceptualFieldKind") 
        ? config.get("conceptualFieldKind").asText() : null;
    String targetFormat = config.has("targetFormat") 
        ? config.get("targetFormat").asText() : null;
    String value = config.has("value") ? config.get("value").asText() : null;

    if (value != null && value.startsWith("$.")) {
      value = resolveJsonPath(value, nodeResults);
    }

    String mapped = enumMapper.map(conceptualFieldKind, targetFormat, value);
    
    ObjectNode result = JacksonUtility.getJsonMapper().createObjectNode();
    result.put("value", mapped);
    return result;
  }

  private JsonNode executeCustomLogic(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    String code = config.has("code") ? config.get("code").asText() : null;
    String language = config.has("language") ? config.get("language").asText() : "ts";
    
    // Extract arguments from config
    Map<String, Object> args = new HashMap<>();
    if (config.has("value")) {
      String value = config.get("value").asText();
      if (value != null && value.startsWith("$.")) {
        value = resolveJsonPath(value, nodeResults);
      }
      args.put("value", value);
    }

    Object result = customLogicExecutor.execute(code, language, args);
    
    ObjectNode resultNode = JacksonUtility.getJsonMapper().createObjectNode();
    resultNode.set("value", JacksonUtility.getJsonMapper().valueToTree(result));
    return resultNode;
  }

  private JsonNode executeApiCall(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    String endpoint = config.has("endpoint") ? config.get("endpoint").asText() : null;
    String method = config.has("method") ? config.get("method").asText() : "GET";
    
    // Resolve params and body (may reference converted values)
    ObjectNode params = config.has("params") && config.get("params").isObject()
        ? (ObjectNode) config.get("params") : JacksonUtility.getJsonMapper().createObjectNode();
    ObjectNode resolvedParams = resolveReferences(params, nodeResults);
    
    JsonNode body = config.has("body") ? config.get("body") : null;
    if (body != null) {
      // Deep resolve body to handle nested structures and substitute values from initialValues
      log.debug("Resolving API call body. Available node results: {}", nodeResults.keySet());
      body = deepResolveReferences(body, nodeResults);
      try {
        log.debug("Resolved body: {}", JacksonUtility.getJsonMapper().writeValueAsString(body));
      } catch (Exception logEx) {
        log.debug("Resolved body: (could not serialize)");
      }
    }

    // Build operation data - operations are registered by operationId, not endpoint
    // The config may have "operationId" or we need to derive it from endpoint
    String operationId = config.has("operationId") 
        ? config.get("operationId").asText() 
        : endpoint; // Fallback to endpoint if operationId not provided
    
    ObjectNode callData = JacksonUtility.getJsonMapper().createObjectNode();
    callData.put("endpoint", endpoint);
    callData.put("method", method);
    callData.set("params", resolvedParams);
    if (body != null) {
      callData.set("body", body);
    }

    // Try to invoke via operation registry using operationId
    try {
      JsonNode result = operationRegistry.invoke(operationId, callData);
      // Extract data field from standard API response format {success: true, data: [...], metadata: {...}}
      if (result != null && result.isObject() && result.has("success") && result.has("data")) {
        JsonNode successNode = result.get("success");
        JsonNode dataNode = result.get("data");
        // If success is true and data is an array, return the data array
        if ((successNode.isBoolean() && successNode.asBoolean()) || 
            (successNode.isTextual() && "true".equalsIgnoreCase(successNode.asText()))) {
          if (dataNode.isArray()) {
            return dataNode;
          }
        }
      }
      return result;
    } catch (Exception e) {
      // If operationId doesn't work, try endpoint as fallback
      if (!operationId.equals(endpoint)) {
        try {
          JsonNode result = operationRegistry.invoke(endpoint, callData);
          // Extract data field from standard API response format
          if (result != null && result.isObject() && result.has("success") && result.has("data")) {
            JsonNode successNode = result.get("success");
            JsonNode dataNode = result.get("data");
            if ((successNode.isBoolean() && successNode.asBoolean()) || 
                (successNode.isTextual() && "true".equalsIgnoreCase(successNode.asText()))) {
              if (dataNode.isArray()) {
                return dataNode;
              }
            }
          }
          return result;
        } catch (Exception e2) {
          throw new DagExecutionException(
              "Failed to invoke API call for operationId: " + operationId + ", endpoint: " + endpoint, e);
        }
      }
      throw new DagExecutionException(
          "Failed to invoke API call for operationId: " + operationId + ", endpoint: " + endpoint, e);
    }
  }

  private JsonNode executeFilter(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    JsonNode source = getInputNodeResult(node, "source", nodeResults);
    if (!source.isArray()) {
      throw new DagExecutionException("Filter source must be an array");
    }

    String field = config.has("field") ? config.get("field").asText() : null;
    String operator = config.has("operator") ? config.get("operator").asText() : null;
    JsonNode valueNode = config.has("value") ? config.get("value") : null;
    Object value = valueNode != null ? extractValue(valueNode, nodeResults) : null;

    ArrayNode filtered = JacksonUtility.getJsonMapper().createArrayNode();
    for (JsonNode item : source) {
      if (matchesFilter(item, field, operator, value)) {
        filtered.add(item);
      }
    }
    return filtered;
  }

  private JsonNode executeProject(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    JsonNode source = getInputNodeResult(node, "source", nodeResults);
    if (!source.isArray()) {
      throw new DagExecutionException("Project source must be an array");
    }

    ArrayNode fields = config.has("fields") && config.get("fields").isArray()
        ? (ArrayNode) config.get("fields") : JacksonUtility.getJsonMapper().createArrayNode();

    ArrayNode projected = JacksonUtility.getJsonMapper().createArrayNode();
    for (JsonNode item : source) {
      ObjectNode projectedItem = JacksonUtility.getJsonMapper().createObjectNode();
      for (JsonNode fieldNode : fields) {
        String fieldName = fieldNode.asText();
        if (item.has(fieldName)) {
          projectedItem.set(fieldName, item.get(fieldName));
        }
      }
      projected.add(projectedItem);
    }
    return projected;
  }

  private JsonNode executeSort(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    JsonNode source = getInputNodeResult(node, "source", nodeResults);
    if (!source.isArray()) {
      throw new DagExecutionException("Sort source must be an array");
    }

    String field = config.has("field") ? config.get("field").asText() : null;
    String direction = config.has("direction") ? config.get("direction").asText() : "asc";

    List<JsonNode> items = new ArrayList<>();
    source.forEach(items::add);

    Comparator<JsonNode> comparator = (a, b) -> {
      JsonNode aVal = a.has(field) ? a.get(field) : null;
      JsonNode bVal = b.has(field) ? b.get(field) : null;
      
      int cmp = compareJsonValues(aVal, bVal);
      return "desc".equals(direction) ? -cmp : cmp;
    };

    items.sort(comparator);

    ArrayNode sorted = JacksonUtility.getJsonMapper().createArrayNode();
    items.forEach(sorted::add);
    return sorted;
  }

  private JsonNode executeAggregate(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    JsonNode source = getInputNodeResult(node, "source", nodeResults);
    if (!source.isArray()) {
      throw new DagExecutionException("Aggregate source must be an array");
    }

    ArrayNode groupBy = config.has("group_by") && config.get("group_by").isArray()
        ? (ArrayNode) config.get("group_by") : JacksonUtility.getJsonMapper().createArrayNode();
    ArrayNode aggregates = config.has("aggregates") && config.get("aggregates").isArray()
        ? (ArrayNode) config.get("aggregates") : JacksonUtility.getJsonMapper().createArrayNode();

    // Group items
    Map<String, List<JsonNode>> groups = new LinkedHashMap<>();
    for (JsonNode item : source) {
      String groupKey = buildGroupKey(item, groupBy);
      groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(item);
    }

    // Aggregate each group
    ArrayNode result = JacksonUtility.getJsonMapper().createArrayNode();
    for (Map.Entry<String, List<JsonNode>> group : groups.entrySet()) {
      ObjectNode aggregated = JacksonUtility.getJsonMapper().createObjectNode();
      
      // Add group-by fields
      JsonNode firstItem = group.getValue().get(0);
      for (JsonNode fieldNode : groupBy) {
        String fieldName = fieldNode.asText();
        if (firstItem.has(fieldName)) {
          aggregated.set(fieldName, firstItem.get(fieldName));
        }
      }

      // Compute aggregates
      for (JsonNode aggNode : aggregates) {
        String field = aggNode.has("field") ? aggNode.get("field").asText() : null;
        String function = aggNode.has("function") ? aggNode.get("function").asText() : null;
        String as = aggNode.has("as") ? aggNode.get("as").asText() : field;

        Object aggValue = computeAggregate(group.getValue(), field, function);
        aggregated.set(as, JacksonUtility.getJsonMapper().valueToTree(aggValue));
      }

      result.add(aggregated);
    }
    return result;
  }

  private JsonNode executeJoin(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    JsonNode left = getInputNodeResult(node, "left", nodeResults);
    JsonNode right = getInputNodeResult(node, "right", nodeResults);
    
    if (!left.isArray() || !right.isArray()) {
      throw new DagExecutionException("Join inputs must be arrays");
    }

    String leftField = config.has("leftField") ? config.get("leftField").asText() : null;
    String rightField = config.has("rightField") ? config.get("rightField").asText() : null;
    String joinType = config.has("joinType") ? config.get("joinType").asText() : "inner";

    // Build index for right side
    Map<Object, List<JsonNode>> rightIndex = new HashMap<>();
    for (JsonNode rightItem : right) {
      Object key = extractFieldValue(rightItem, rightField);
      rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(rightItem);
    }

    ArrayNode result = JacksonUtility.getJsonMapper().createArrayNode();
    for (JsonNode leftItem : left) {
      Object key = extractFieldValue(leftItem, leftField);
      List<JsonNode> matches = rightIndex.getOrDefault(key, Collections.emptyList());
      
      if (matches.isEmpty() && "inner".equals(joinType)) {
        continue; // Skip for inner join
      }
      
      if (matches.isEmpty()) {
        // Left/Full join: add left item with null right fields
        ObjectNode joined = (ObjectNode) leftItem.deepCopy();
        result.add(joined);
      } else {
        // Add joined items
        for (JsonNode rightItem : matches) {
          ObjectNode joined = (ObjectNode) leftItem.deepCopy();
          rightItem.fields().forEachRemaining(entry -> {
            joined.set("right_" + entry.getKey(), entry.getValue());
          });
          result.add(joined);
        }
      }
    }
    return result;
  }

  private JsonNode executeLimitOffset(DagNode node, ObjectNode config, 
      Map<String, JsonNode> nodeResults) {
    JsonNode source = getInputNodeResult(node, "source", nodeResults);
    if (!source.isArray()) {
      throw new DagExecutionException("LimitOffset source must be an array");
    }

    int limit = config.has("limit") ? config.get("limit").asInt() : Integer.MAX_VALUE;
    int offset = config.has("offset") ? config.get("offset").asInt() : 0;

    ArrayNode result = JacksonUtility.getJsonMapper().createArrayNode();
    int index = 0;
    for (JsonNode item : source) {
      if (index >= offset && result.size() < limit) {
        result.add(item);
      }
      index++;
    }
    return result;
  }

  // Helper methods

  private List<String> topologicalSort(List<DagNode> nodes, Map<String, DagNode> nodeMap) {
    Map<String, Integer> inDegree = new HashMap<>();
    Map<String, List<String>> edges = new HashMap<>();

    // Initialize
    for (DagNode node : nodes) {
      inDegree.put(node.getId(), 0);
      edges.put(node.getId(), new ArrayList<>());
    }

    // Build graph from explicit inputs
    for (DagNode node : nodes) {
      for (String inputNodeId : node.getInputs().values()) {
        inDegree.put(node.getId(), inDegree.get(node.getId()) + 1);
        edges.computeIfAbsent(inputNodeId, k -> new ArrayList<>()).add(node.getId());
      }
    }

    // Also extract implicit dependencies from JSONPath references in configs
    // This ensures nodes referenced via JSONPath (e.g., "$.cv1.value") are executed first
    for (DagNode node : nodes) {
      Set<String> referencedNodeIds = extractJsonPathReferences(node.getConfig(), nodeMap.keySet());
      for (String referencedNodeId : referencedNodeIds) {
        // Only add edge if referenced node exists and isn't already an explicit input
        if (nodeMap.containsKey(referencedNodeId) && 
            !node.getInputs().containsValue(referencedNodeId)) {
          inDegree.put(node.getId(), inDegree.get(node.getId()) + 1);
          edges.computeIfAbsent(referencedNodeId, k -> new ArrayList<>()).add(node.getId());
        }
      }
    }

    // Kahn's algorithm
    LinkedList<String> queue = new LinkedList<>();
    for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
      if (entry.getValue() == 0) {
        queue.add(entry.getKey());
      }
    }

    List<String> result = new ArrayList<>();
    while (!queue.isEmpty()) {
      String nodeId = queue.poll();
      result.add(nodeId);

      for (String neighbor : edges.getOrDefault(nodeId, Collections.emptyList())) {
        inDegree.put(neighbor, inDegree.get(neighbor) - 1);
        if (inDegree.get(neighbor) == 0) {
          queue.add(neighbor);
        }
      }
    }

    if (result.size() != nodes.size()) {
      throw new DagExecutionException("DAG contains cycles");
    }

    return result;
  }

  /**
   * Extract node IDs referenced via JSONPath from a config node.
   * This finds all strings like "$.nodeId.field" and returns the node IDs.
   * 
   * @param config the config JsonNode to search
   * @param validNodeIds set of valid node IDs (to filter out _initial and other special references)
   * @return set of node IDs referenced in the config
   */
  private Set<String> extractJsonPathReferences(JsonNode config, Set<String> validNodeIds) {
    Set<String> referencedNodeIds = new HashSet<>();
    
    if (config == null || config.isNull()) {
      return referencedNodeIds;
    }
    
    if (config.isTextual()) {
      String value = config.asText();
      if (value.startsWith("$.")) {
        // Extract node ID from JSONPath like "$.cv1.value" or "$.nodeId.field"
        String[] parts = value.substring(2).split("\\.");
        if (parts.length >= 1) {
          String nodeId = parts[0];
          // Only include if it's a valid node ID (not _initial or other special references)
          if (validNodeIds.contains(nodeId)) {
            referencedNodeIds.add(nodeId);
          }
        }
      }
    } else if (config.isObject()) {
      config.fields().forEachRemaining(entry -> {
        referencedNodeIds.addAll(extractJsonPathReferences(entry.getValue(), validNodeIds));
      });
    } else if (config.isArray()) {
      for (JsonNode item : config) {
        referencedNodeIds.addAll(extractJsonPathReferences(item, validNodeIds));
      }
    }
    
    return referencedNodeIds;
  }

  private JsonNode getInputNodeResult(DagNode node, String inputName, 
      Map<String, JsonNode> nodeResults) {
    String sourceNodeId = node.getInputs().get(inputName);
    if (sourceNodeId == null) {
      throw new DagExecutionException("Missing input: " + inputName + " for node " + node.getId());
    }
    JsonNode result = nodeResults.get(sourceNodeId);
    if (result == null) {
      throw new DagExecutionException("Source node result not found: " + sourceNodeId);
    }
    return result;
  }

  private ObjectNode resolveReferences(ObjectNode node, Map<String, JsonNode> nodeResults) {
    return (ObjectNode) deepResolveReferences(node, nodeResults);
  }

  /**
   * Deeply resolve references in a JSON node, handling objects, arrays, and all value types.
   * This ensures that values from initialValues are properly substituted even when cached plans
   * have hardcoded values.
   * 
   * CRITICAL: When a filter has a hardcoded value that matches a value in initialValues,
   * we need to substitute it. However, we can't blindly substitute all numeric values.
   * The proper fix is for the LLM to generate JSONPath references (e.g., "$._initial.v1"),
   * but as a workaround, we check if the value in a filter field matches an initial value
   * and substitute it.
   */
  private JsonNode deepResolveReferences(JsonNode node, Map<String, JsonNode> nodeResults) {
    if (node == null || node.isNull()) {
      return node;
    }
    
    if (node.isTextual()) {
      String value = node.asText();
        if (value.startsWith("$.")) {
          String resolvedValue = resolveJsonPath(value, nodeResults);
        return JacksonUtility.getJsonMapper().valueToTree(resolvedValue);
      }
      return node;
    }
    
    if (node.isObject()) {
      ObjectNode resolved = JacksonUtility.getJsonMapper().createObjectNode();
      node.fields().forEachRemaining(entry -> {
        String key = entry.getKey();
        JsonNode value = entry.getValue();
        
        // Special handling for filter arrays in API call bodies
        // If we have a filter with a hardcoded value, try to substitute from initialValues
        if ("filter".equals(key) && value.isArray()) {
          ArrayNode filterArray = (ArrayNode) value;
          ArrayNode resolvedFilter = JacksonUtility.getJsonMapper().createArrayNode();
          for (JsonNode filterItem : filterArray) {
            if (filterItem.isObject()) {
              ObjectNode resolvedFilterItem = substituteFilterValue((ObjectNode) filterItem, nodeResults);
              resolvedFilter.add(resolvedFilterItem);
            } else {
              resolvedFilter.add(deepResolveReferences(filterItem, nodeResults));
            }
          }
          resolved.set(key, resolvedFilter);
        } else {
          JsonNode resolvedValue = deepResolveReferences(value, nodeResults);
          resolved.set(key, resolvedValue);
        }
      });
      return resolved;
    }
    
    if (node.isArray()) {
      ArrayNode resolved = JacksonUtility.getJsonMapper().createArrayNode();
      for (JsonNode item : node) {
        JsonNode resolvedItem = deepResolveReferences(item, nodeResults);
        resolved.add(resolvedItem);
      }
      return resolved;
    }
    
    return node;
  }

  /**
   * Substitute hardcoded filter values with values from initialValues.
   * This is a workaround for cached plans that have hardcoded values instead of JSONPath references.
   * 
   * When a filter has a hardcoded value in a date-related field, we substitute it with
   * the corresponding value from initialValues. This handles the case where cached plans
   * have hardcoded values (e.g., 2024) that should be dynamic (e.g., from v1 in initialValues).
   */
  private ObjectNode substituteFilterValue(ObjectNode filterItem, Map<String, JsonNode> nodeResults) {
    ObjectNode resolved = filterItem.deepCopy();
    
    // Always resolve the "value" field recursively to handle JSONPath references
    // This handles cases where value is a string, number, array, or object
    if (resolved.has("value")) {
      JsonNode valueNode = resolved.get("value");
      JsonNode resolvedValue = deepResolveReferences(valueNode, nodeResults);
      resolved.set("value", resolvedValue);
    }
    
    // Also handle hardcoded value substitution for date-related fields (legacy workaround)
    JsonNode initialNode = nodeResults.get("_initial");
    if (initialNode != null && initialNode.isObject()) {
      if (resolved.has("value")) {
        JsonNode valueNode = resolved.get("value");
        String fieldName = resolved.has("field") ? resolved.get("field").asText() : null;
        
        // If this is a date-related field (date.year, date_yyyy, etc.), the value should be dynamic
        if (fieldName != null && (fieldName.contains("date") || fieldName.contains("year") || 
            fieldName.contains("month") || fieldName.contains("day") || fieldName.contains("quarter"))) {
          
          // For date-related filters with hardcoded values, we need to substitute with current initialValues
          // The cached plan may have hardcoded "2024" but we're now querying for "2022"
          // Strategy: If the hardcoded value is a number that looks like a year (4 digits, >= 1900),
          // and we have initial values, use the first initial value that also looks like a year
          if (valueNode.isNumber()) {
            int hardcodedYear = valueNode.asInt();
            // Check if this looks like a year (reasonable range)
            if (hardcodedYear >= 1900 && hardcodedYear <= 2100) {
              // Find the first initial value that looks like a year
              java.util.Iterator<java.util.Map.Entry<String, JsonNode>> fields = initialNode.fields();
              while (fields.hasNext()) {
                java.util.Map.Entry<String, JsonNode> entry = fields.next();
                String initialValue = entry.getValue().asText();
                try {
                  int initialYear = Integer.parseInt(initialValue);
                  if (initialYear >= 1900 && initialYear <= 2100) {
                    // Found a year-like initial value - substitute it
                    resolved.put("value", initialYear);
                    break;
                  }
                } catch (NumberFormatException e) {
                  // Not a number, skip
                }
              }
            }
          } else if (valueNode.isTextual() && !valueNode.asText().startsWith("$.")) {
            // String value (not a JSONPath) - try to match with initial values
            String hardcodedValue = valueNode.asText();
            java.util.Iterator<java.util.Map.Entry<String, JsonNode>> fields = initialNode.fields();
            while (fields.hasNext()) {
              java.util.Map.Entry<String, JsonNode> entry = fields.next();
              String initialValue = entry.getValue().asText();
              // If values match or both look like years, substitute
              if (initialValue.equals(hardcodedValue) || 
                  (isYearLike(hardcodedValue) && isYearLike(initialValue))) {
                resolved.put("value", initialValue);
                break;
              }
            }
          }
        }
      }
    }
    
    return resolved;
  }

  /**
   * Check if a string value looks like a year (4 digits, reasonable range).
   */
  private boolean isYearLike(String value) {
    if (value == null || value.length() != 4) {
      return false;
    }
    try {
      int year = Integer.parseInt(value);
      return year >= 1900 && year <= 2100;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private String resolveJsonPath(String path, Map<String, JsonNode> nodeResults) {
    // Simple JSONPath resolution: $.nodeId.field or $.nodeId.value
    if (path.startsWith("$.")) {
      String[] parts = path.substring(2).split("\\.");
      if (parts.length >= 1) {
        String nodeId = parts[0];
        JsonNode nodeResult = nodeResults.get(nodeId);
        
        if (nodeResult == null) {
          log.warn("Node result not found for JSONPath reference: {} (available nodes: {})", 
              path, nodeResults.keySet());
          return path; // Return original path if node not found
        }
        
        if (parts.length > 1) {
          // Has field specifier: $.nodeId.field
          String field = parts[1];
          if (nodeResult.has(field)) {
            JsonNode fieldValue = nodeResult.get(field);
            if (fieldValue.isTextual()) {
              return fieldValue.asText();
            } else if (fieldValue.isNumber()) {
              return fieldValue.asText();
            } else if (fieldValue.isBoolean()) {
              return String.valueOf(fieldValue.asBoolean());
            } else {
              return fieldValue.toString();
            }
          } else {
            log.warn("Field '{}' not found in node result for JSONPath: {}", field, path);
            return path;
          }
        } else {
          // No field specifier: $.nodeId - try to get "value" field
          if (nodeResult.has("value")) {
            JsonNode valueNode = nodeResult.get("value");
            if (valueNode.isTextual()) {
              return valueNode.asText();
            } else if (valueNode.isNumber()) {
              return valueNode.asText();
            } else if (valueNode.isBoolean()) {
              return String.valueOf(valueNode.asBoolean());
            } else {
              return valueNode.toString();
            }
          } else {
            log.warn("No 'value' field found in node result for JSONPath: {}", path);
            return path;
          }
        }
      }
    }
    return path;
  }

  private Object extractValue(JsonNode valueNode, Map<String, JsonNode> nodeResults) {
    if (valueNode.isTextual()) {
      String value = valueNode.asText();
      if (value.startsWith("$.")) {
        return resolveJsonPath(value, nodeResults);
      }
      return value;
    }
    return JacksonUtility.getJsonMapper().convertValue(valueNode, Object.class);
  }

  private boolean matchesFilter(JsonNode item, String field, String operator, Object value) {
    if (field == null || operator == null) {
      return true;
    }

    JsonNode fieldValue = item.has(field) ? item.get(field) : null;
    Object itemValue = fieldValue != null ? extractJsonValue(fieldValue) : null;

    switch (operator) {
      case "equals":
        return compareValues(itemValue, value) == 0;
      case "not_equals":
        return compareValues(itemValue, value) != 0;
      case "greater_than":
        return compareValues(itemValue, value) > 0;
      case "greater_than_or_equal":
        return compareValues(itemValue, value) >= 0;
      case "less_than":
        return compareValues(itemValue, value) < 0;
      case "less_than_or_equal":
        return compareValues(itemValue, value) <= 0;
      case "contains":
        return itemValue != null && String.valueOf(itemValue).contains(String.valueOf(value));
      default:
        return false;
    }
  }

  private Object extractJsonValue(JsonNode node) {
    if (node.isTextual()) return node.asText();
    if (node.isNumber()) return node.asDouble();
    if (node.isBoolean()) return node.asBoolean();
    return node.asText();
  }

  private int compareValues(Object a, Object b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    if (a instanceof Number && b instanceof Number) {
      return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
    }
    return String.valueOf(a).compareTo(String.valueOf(b));
  }

  private int compareJsonValues(JsonNode a, JsonNode b) {
    Object aVal = a != null ? extractJsonValue(a) : null;
    Object bVal = b != null ? extractJsonValue(b) : null;
    return compareValues(aVal, bVal);
  }

  private String buildGroupKey(JsonNode item, ArrayNode groupBy) {
    List<String> keyParts = new ArrayList<>();
    for (JsonNode fieldNode : groupBy) {
      String fieldName = fieldNode.asText();
      if (item.has(fieldName)) {
        keyParts.add(item.get(fieldName).asText());
      } else {
        keyParts.add("");
      }
    }
    return String.join("|", keyParts);
  }

  private Object extractFieldValue(JsonNode item, String field) {
    if (field == null) return null;
    String[] parts = field.split("\\.");
    JsonNode current = item;
    for (String part : parts) {
      if (current != null && current.has(part)) {
        current = current.get(part);
      } else {
        return null;
      }
    }
    return extractJsonValue(current);
  }

  private Object computeAggregate(List<JsonNode> items, String field, String function) {
    if (function == null) return null;

    switch (function) {
      case "sum":
        return items.stream()
            .mapToDouble(item -> {
              JsonNode val = item.has(field) ? item.get(field) : null;
              return val != null && val.isNumber() ? val.asDouble() : 0.0;
            })
            .sum();
      case "avg":
      case "average":
        double sum = items.stream()
            .mapToDouble(item -> {
              JsonNode val = item.has(field) ? item.get(field) : null;
              return val != null && val.isNumber() ? val.asDouble() : 0.0;
            })
            .sum();
        return items.isEmpty() ? 0.0 : sum / items.size();
      case "min":
        return items.stream()
            .map(item -> {
              JsonNode val = item.has(field) ? item.get(field) : null;
              return val != null && val.isNumber() ? val.asDouble() : Double.MAX_VALUE;
            })
            .min(Double::compare)
            .orElse(0.0);
      case "max":
        return items.stream()
            .map(item -> {
              JsonNode val = item.has(field) ? item.get(field) : null;
              return val != null && val.isNumber() ? val.asDouble() : Double.MIN_VALUE;
            })
            .max(Double::compare)
            .orElse(0.0);
      case "count":
        return items.size();
      default:
        return null;
    }
  }

  public static class DagExecutionException extends RuntimeException {
    public DagExecutionException(String message) {
      super(message);
    }

    public DagExecutionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}

