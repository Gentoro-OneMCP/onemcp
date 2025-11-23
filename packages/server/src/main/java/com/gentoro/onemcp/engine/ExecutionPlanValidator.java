package com.gentoro.onemcp.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 * Utility to validate the structure of an execution-plan definition before execution.
 *
 * <p>This validator focuses on fast, predictable, <em>structural</em> checks that can be run before
 * the plan reaches the runtime. It ensures that all required fields exist and have the expected
 * shape and type, that referenced nodes are declared, and that node-specific contracts are
 * respected. This class does not perform any data-dependent or expression evaluation — JSONPath
 * expressions, boolean predicates, or arithmetic are validated at runtime by {@link
 * ExecutionPlanEngine}.
 *
 * <p>The validator is intentionally conservative to keep error messages actionable and to prevent
 * costly execution attempts of malformed plans. All validation failures are reported as {@link
 * ExecutionPlanException} with descriptive messages aimed at users and logs.
 *
 * <p>Minimal example of a valid plan shape:
 *
 * <pre>{@code
 * {
 *   "start_node": "fetch",
 *   "nodes": {
 *     "fetch": {"type": "call", "operation": "orders.findAll", "next": "summary"},
 *     "summary": {"type": "summary", "output": {"orders": "$._gstt.fetch.value"}}
 *   }
 * }
 * }</pre>
 */
public final class ExecutionPlanValidator {

  private ExecutionPlanValidator() {}

  /**
   * Validate the given execution-plan definition.
   *
   * <p>Throws {@link ExecutionPlanException} when the plan is structurally invalid. This method
   * performs the following high-level checks:
   *
   * <ul>
   *   <li>Root object existence and type
   *   <li>Presence of {@code nodes} and {@code start_node}
   *   <li>Each declared node has a supported {@code type}
   *   <li>Node-type specific required fields and their types
   *   <li>That any {@code next} routing points to declared nodes
   * </ul>
   */
  public static void validate(JsonNode plan, List<String> listOfAllowedOperations) {
    if (plan == null || !plan.isObject()) {
      throw new ExecutionPlanException("Execution plan must be a non-null JSON object");
    }

    // Legacy schema with 'nodes' is no longer supported.
    if (plan.has("nodes")) {
      throw new ExecutionPlanException(
          "Legacy execution plan schema with 'nodes' is no longer supported. Use 'start_node' object and top-level node entries.");
    }

    validateNewSpec(plan, listOfAllowedOperations);
  }

  /**
   * Backward-compatible overload: validates structure only, without enforcing allowed operations.
   * If you need operation whitelist enforcement, call {@link #validate(JsonNode, List)} with a
   * non-empty list.
   */
  public static void validate(JsonNode plan) {
    validate(plan, java.util.Collections.emptyList());
  }

  private static void validateLegacy(JsonNode plan, List<String> allowedOperations) {
    JsonNode nodesNode = plan.get("nodes");
    if (nodesNode == null || !nodesNode.isObject()) {
      throw new ExecutionPlanException("Execution plan is missing 'nodes' object");
    }
    ObjectNode nodes = (ObjectNode) nodesNode;

    JsonNode startNodeName = plan.get("start_node");
    if (startNodeName == null || !startNodeName.isTextual()) {
      throw new ExecutionPlanException("Execution plan is missing textual 'start_node'");
    }
    String startId = startNodeName.asText();

    int definedNodes = 0;
    for (String fieldName : iterable(nodes.fieldNames())) {
      definedNodes++;
    }
    if (definedNodes == 0) {
      throw new ExecutionPlanException(
          "Execution plan must define at least one node under 'nodes'");
    }
    if (!nodes.has(startId)) {
      throw new ExecutionPlanException(
          "Execution plan start node '" + startId + "' is not defined under 'nodes'");
    }

    for (String nodeId : iterable(nodes.fieldNames())) {
      if ("start_node".equals(nodeId)) {
        continue;
      }
      JsonNode nodeDef = nodes.get(nodeId);
      if (nodeDef == null || !nodeDef.isObject()) {
        throw new ExecutionPlanException("Node '" + nodeId + "' definition must be a JSON object");
      }
      String type = nodeDef.has("type") ? nodeDef.get("type").asText() : null;
      if (type == null && "summary".equals(nodeId)) {
        type = "summary"; // summary type is implicit
      }
      if (type == null) {
        throw new ExecutionPlanException("Node '" + nodeId + "' is missing required 'type' field");
      }
      switch (type) {
        case "call" -> validateCallNode(nodeId, nodeDef, nodes, allowedOperations);
        case "map" -> validateMapNode(nodeId, nodeDef, nodes);
        case "sort" -> validateSortNode(nodeId, nodeDef, nodes);
        case "group" -> validateGroupNode(nodeId, nodeDef, nodes);
        case "filter" -> validateFilterNode(nodeId, nodeDef, nodes);
        case "concat" -> validateConcatNode(nodeId, nodeDef, nodes);
        case "combine" -> validateCombineNode(nodeId, nodeDef, nodes);
        case "expr" -> validateExprNode(nodeId, nodeDef, nodes);
        case "summary" -> validateSummaryNode(nodeId, nodeDef);
        default -> throw new ExecutionPlanException(
            "Node '" + nodeId + "' has unsupported type '" + type + "'");
      }
    }
  }

  // ---------------- New-spec validator ----------------
  private static void validateNewSpec(JsonNode plan, List<String> allowedOperations) {
    JsonNode start = plan.get("start_node");
    if (start == null || !start.isObject()) {
      throw new ExecutionPlanException("New-spec plan must define object 'start_node'");
    }

    // start_node.vars (optional) must be object if present
    JsonNode vars = start.get("vars");
    if (vars != null && !vars.isObject()) {
      throw new ExecutionPlanException("'start_node.vars' must be an object when present");
    }

    // start_node.route is required: string, object(singleton), or array
    JsonNode route = start.get("route");
    if (route == null || !(route.isTextual() || route.isArray() || route.isObject())) {
      throw new ExecutionPlanException(
          "'start_node.route' must be a string (next), an object with a single entry, or an array of conditions with fallback");
    }

    // Ensure there is at least one intermediate or terminal node
    int nodeCount = 0;
    for (String key : iterable(plan.fieldNames())) {
      if (!"start_node".equals(key)) nodeCount++;
    }
    if (nodeCount == 0) {
      throw new ExecutionPlanException("Plan must define at least one node besides 'start_node'");
    }

    // Validate each node (intermediate or terminal)
    for (String nodeId : iterable(plan.fieldNames())) {
      if ("start_node".equals(nodeId)) continue;
      JsonNode nodeDef = plan.get(nodeId);
      if (nodeDef == null || !nodeDef.isObject()) {
        throw new ExecutionPlanException("Node '" + nodeId + "' must be a JSON object");
      }
      boolean isTerminal =
          nodeDef.has("completed")
              && nodeDef.get("completed").isBoolean()
              && nodeDef.get("completed").asBoolean();
      if (isTerminal) {
        JsonNode v = nodeDef.get("vars");
        if (v != null && !v.isObject()) {
          throw new ExecutionPlanException(
              "Terminal node '" + nodeId + "' has invalid 'vars': expected object");
        }
        continue; // no further checks
      }
      // Intermediate operation node
      JsonNode op = nodeDef.get("operation");
      if (op == null || !op.isTextual() || op.asText().isBlank()) {
        throw new ExecutionPlanException(
            "Node '"
                + nodeId
                + "' must define non-empty textual 'operation' or set completed=true");
      }
      // Enforce allowed operations whitelist
      String opName = op.asText();
      if (allowedOperations != null
          && !allowedOperations.isEmpty()
          && !allowedOperations.contains(opName)) {
        throw new ExecutionPlanException(
            "Node '"
                + nodeId
                + "' uses unsupported operation '"
                + opName
                + "'. Allowed operations: "
                + String.valueOf(allowedOperations));
      }
      JsonNode input = nodeDef.get("input");
      if (input != null && !(input.isObject() || input.isTextual())) {
        throw new ExecutionPlanException(
            "Node '" + nodeId + "' has invalid 'input': expected object or string");
      }
      JsonNode r = nodeDef.get("route");
      if (r == null || !(r.isTextual() || r.isArray() || r.isObject())) {
        throw new ExecutionPlanException(
            "Node '" + nodeId + "' must define 'route' as string, object(single entry) or array");
      }
    }

    // Validate that all referenced node ids in start_node.route and nodes' routes exist
    // For arrays: allow one or more objects with 'condition' and 'node', plus final fallback string
    for (String nodeId : iterable(plan.fieldNames())) {
      JsonNode r =
          "start_node".equals(nodeId)
              ? plan.get("start_node").get("route")
              : plan.get(nodeId).get("route");
      if (r == null) continue;
      if (r.isTextual()) {
        ensureNodeExists(plan, nodeId, r.asText());
      } else if (r.isObject()) {
        // must be a single entry whose value is a textual node id
        int fields = 0;
        String target = null;
        for (String k : iterable(r.fieldNames())) {
          fields++;
          if (fields > 1) break;
          JsonNode v = r.get(k);
          if (v != null && v.isTextual()) {
            target = v.asText();
          }
        }
        if (fields != 1 || target == null) {
          throw new ExecutionPlanException(
              "'"
                  + nodeId
                  + "'.route object form must contain exactly one entry with a textual value (node id)");
        }
        ensureNodeExists(plan, nodeId, target);
      } else if (r.isArray()) {
        int sz = r.size();
        if (sz == 0) {
          throw new ExecutionPlanException(
              "'" + nodeId + "'.route array must not be empty and must end with a fallback string");
        }
        for (int i = 0; i < sz - 1; i++) {
          JsonNode condEntry = r.get(i);
          if (!condEntry.isObject()
              || !condEntry.has("node")
              || !condEntry.get("node").isTextual()
              || !condEntry.has("condition")
              || !condEntry.get("condition").isTextual()) {
            throw new ExecutionPlanException(
                "'"
                    + nodeId
                    + "'.route["
                    + i
                    + "] must be an object with textual 'condition' and 'node'");
          }
          ensureNodeExists(plan, nodeId, condEntry.get("node").asText());
        }
        JsonNode fallback = r.get(sz - 1);
        if (!fallback.isTextual()) {
          throw new ExecutionPlanException(
              "'" + nodeId + "'.route must end with a fallback node id string");
        }
        ensureNodeExists(plan, nodeId, fallback.asText());
      }
    }
  }

  private static void ensureNodeExists(JsonNode plan, String fromNodeId, String targetId) {
    if (!plan.has(targetId)) {
      throw new ExecutionPlanException(
          "Route from '" + fromNodeId + "' references missing node '" + targetId + "'");
    }
  }

  private static void validateCallNode(
      String nodeId, JsonNode nodeDef, ObjectNode nodes, List<String> allowedOperations) {
    // A call node triggers a registered Operation by name and may shape input/output.
    JsonNode op = nodeDef.get("operation");
    if (op == null || !op.isTextual() || op.asText().isEmpty()) {
      throw new ExecutionPlanException(
          "call node '" + nodeId + "' must define non-empty textual 'operation'");
    }
    // Enforce allowed operations whitelist for legacy schema
    String opName = op.asText();
    if (allowedOperations != null
        && !allowedOperations.isEmpty()
        && !allowedOperations.contains(opName)) {
      throw new ExecutionPlanException(
          "call node '"
              + nodeId
              + "' uses unsupported operation '"
              + opName
              + "'. Allowed operations: "
              + String.valueOf(allowedOperations));
    }

    JsonNode input = nodeDef.get("input");
    if (input != null && !(input.isObject() || input.isTextual())) {
      throw new ExecutionPlanException(
          "call node '" + nodeId + "' has invalid 'input': expected object or string");
    }

    JsonNode output = nodeDef.get("output");
    if (output != null && !(output.isObject() || output.isTextual())) {
      throw new ExecutionPlanException(
          "call node '" + nodeId + "' has invalid 'output': expected object or string");
    }

    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateMapNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // A map node iterates a collection input and executes a single inner node for each element.
    JsonNode input = nodeDef.get("input");
    if (input == null) {
      throw new ExecutionPlanException("map node '" + nodeId + "' must define 'input'");
    }
    if (!(input.isTextual() || input.isObject())) {
      throw new ExecutionPlanException(
          "map node '" + nodeId + "' has invalid 'input': expected string or object");
    }
    JsonNode foreach = nodeDef.get("foreach");
    if (foreach == null || !foreach.isObject()) {
      throw new ExecutionPlanException("map node '" + nodeId + "' must define 'foreach' object");
    }
    JsonNode foreachNode = foreach.get("node");
    if (foreachNode == null || !foreachNode.isTextual() || foreachNode.asText().isEmpty()) {
      throw new ExecutionPlanException(
          "map node '" + nodeId + "' must define non-empty textual 'foreach.node'");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateSortNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // A sort node sorts an incoming array using a JSONPath expression as the key selector.
    if (!nodeDef.has("input")) {
      throw new ExecutionPlanException("sort node '" + nodeId + "' must define 'input'");
    }
    if (!nodeDef.has("expr")
        || !nodeDef.get("expr").isTextual()
        || nodeDef.get("expr").asText().isEmpty()) {
      throw new ExecutionPlanException("sort node '" + nodeId + "' must define textual 'expr'");
    }
    if (nodeDef.has("dir") && !nodeDef.get("dir").isTextual()) {
      throw new ExecutionPlanException(
          "sort node '" + nodeId + "' has invalid 'dir': expected one of 'asc','desc'");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateGroupNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // A group node transforms each item into a key/value record based on provided expressions.
    if (!nodeDef.has("input")) {
      throw new ExecutionPlanException("group node '" + nodeId + "' must define 'input'");
    }
    JsonNode key = nodeDef.get("key");
    if (key == null || !key.isArray() || key.isEmpty()) {
      throw new ExecutionPlanException(
          "group node '" + nodeId + "' must define non-empty 'key' array");
    }
    if (nodeDef.has("value") && !nodeDef.get("value").isArray()) {
      throw new ExecutionPlanException(
          "group node '" + nodeId + "' has invalid 'value': expected array");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateFilterNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // A filter node keeps items from an array based on a boolean expression.
    if (!nodeDef.has("input")) {
      throw new ExecutionPlanException("filter node '" + nodeId + "' must define 'input'");
    }
    JsonNode expr = nodeDef.get("expr");
    if (expr == null) {
      throw new ExecutionPlanException("filter node '" + nodeId + "' must define 'expr'");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateConcatNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // A concat node builds a string by conditionally appending literals or expression values.
    if (!nodeDef.has("concat") || !nodeDef.get("concat").isArray()) {
      throw new ExecutionPlanException("concat node '" + nodeId + "' must define 'concat' array");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateCombineNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // A combine node performs stepwise joins/merges over arrays referenced by JSONPath expressions.
    if (!nodeDef.has("input") || !nodeDef.get("input").isObject()) {
      throw new ExecutionPlanException("combine node '" + nodeId + "' must define object 'input'");
    }
    if (!nodeDef.has("combine")
        || !nodeDef.get("combine").isArray()
        || nodeDef.get("combine").isEmpty()) {
      throw new ExecutionPlanException(
          "combine node '" + nodeId + "' must define non-empty 'combine' array");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateExprNode(String nodeId, JsonNode nodeDef, ObjectNode nodes) {
    // An expr node computes numeric expressions and stores results under aliases.
    JsonNode expr = nodeDef.get("expr");
    if (expr == null || !expr.isArray() || expr.isEmpty()) {
      throw new ExecutionPlanException(
          "expr node '" + nodeId + "' must define non-empty 'expr' array");
    }
    validateNext(nodeId, nodeDef.get("next"), nodes, true);
  }

  private static void validateSummaryNode(String nodeId, JsonNode nodeDef) {
    if (nodeDef.has("next")) {
      throw new ExecutionPlanException("summary node must not define 'next'");
    }
    JsonNode output = nodeDef.get("output");
    if (output != null && !output.isObject()) {
      throw new ExecutionPlanException("summary node 'output' must be an object when present");
    }
  }

  /**
   * Validate a {@code next} specification.
   *
   * @param allowMissing if true, missing/empty next is allowed (not used currently)
   */
  private static void validateNext(
      String nodeId, JsonNode nextNode, ObjectNode nodes, boolean allowMissing) {
    if (nextNode == null || nextNode.isNull()) {
      if (allowMissing) {
        return;
      }
      // In the simplified spec, missing next means completion is possible.
      return;
    }

    if (nextNode.isTextual()) {
      String target = nextNode.asText();
      validateTargetNode(nodeId, target, nodes);
      return;
    }

    if (nextNode.isArray()) {
      // Conditional routing: an array of {"node": "id", "expr": "$..."} with at least one entry.
      if (nextNode.isEmpty()) {
        throw new ExecutionPlanException(
            "Node '" + nodeId + "' has empty 'next' array; at least one route is required");
      }
      for (JsonNode route : nextNode) {
        JsonNode targetNode = route.get("node");
        if (targetNode == null || !targetNode.isTextual() || targetNode.asText().isEmpty()) {
          throw new ExecutionPlanException(
              "Node '" + nodeId + "' has route without valid 'node' field in 'next' array");
        }
        String target = targetNode.asText();
        validateTargetNode(nodeId, target, nodes);
        JsonNode expr = route.get("expr");
        if (expr != null && !expr.isTextual()) {
          throw new ExecutionPlanException(
              "Node '" + nodeId + "' has invalid 'expr' in 'next' route: expected string");
        }
      }
      return;
    }

    throw new ExecutionPlanException(
        "Node '" + nodeId + "' has invalid 'next' specification; expected string or array");
  }

  private static void validateTargetNode(String fromNodeId, String targetId, ObjectNode nodes) {
    if (!nodes.has(targetId)) {
      throw new ExecutionPlanException(
          "Node '" + fromNodeId + "' routes to undefined node '" + targetId + "'");
    }
  }

  /** Helper to adapt an Iterator to enhanced for-loop. */
  private static Iterable<String> iterable(java.util.Iterator<String> it) {
    return () -> it;
  }
}
