package com.gentoro.onemcp.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core execution engine for JSON-based execution plans.
 *
 * <p>Plans and state are represented as Jackson {@link JsonNode} objects. The engine executes nodes
 * starting from {@code plan.start_node} and routes execution until a terminal node is reached
 * (either an explicit {@code summary} node or an implicit end when a node has no {@code next}).
 * Each node execution can read and modify a shared global state object (internally kept by the
 * engine). In the evaluation context exposed to JsonPath, node results are available directly at
 * the root by their node ids (e.g., {@code $.start_node}, {@code $.my_node}). No special prefixes
 * like {@code _gstt} or {@code _lstt} are required.
 *
 * <p>State and context conventions:
 *
 * <ul>
 *   <li>Global state is tracked internally. For JsonPath evaluation, each node’s output is exposed
 *       at the root under its node id (e.g., {@code $.<nodeId>}).
 *   <li>There is no longer a need for special root entries like {@code $._gstt} or {@code $._lstt}.
 * </ul>
 *
 * <p>This class is intentionally stateless aside from its collaborators; callers are expected to
 * provide an {@link OperationRegistry} containing all domain-specific operations that may be
 * referenced by the plan (via {@code call} and {@code map.foreach} nodes).
 *
 * <p>Node types supported by the engine:
 *
 * <ul>
 *   <li>{@code call}: invoke an operation from the registry
 *   <li>{@code map}: iterate over array/object and execute a single inner node for each item
 *   <li>{@code sort}: sort an array by key selector expression
 *   <li>{@code group}: transform items into key/value records
 *   <li>{@code filter}: keep items matching a boolean predicate
 *   <li>{@code concat}: build a string from conditional parts
 *   <li>{@code combine}: perform stepwise joins/merges using aliases
 *   <li>{@code expr}: compute numeric expressions and store under aliases
 *   <li>{@code summary}: build final output object
 * </ul>
 */
public class ExecutionPlanEngine {

  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanEngine.class);
  private final JsonPathResolver jsonPath;
  private final OperationRegistry registry;

  /**
   * Create a new engine instance.
   *
   * @param mapper {@link ObjectMapper} used for plan loading and JsonLogic integration.
   * @param registry registry of operations that can be called from {@code operation_call} and
   *     {@code iterate} nodes.
   */
  public ExecutionPlanEngine(ObjectMapper mapper, OperationRegistry registry) {
    this.registry = registry;
    this.jsonPath = new JsonPathResolver();
  }

  /**
   * Execute a validated plan.
   *
   * <p>The method first delegates to {@link ExecutionPlanValidator#validate(JsonNode)} to ensure
   * the plan is structurally sound, then performs node-by-node execution until the {@code summary}
   * node is reached.
   *
   * @param plan execution-plan definition as a JSON object.
   * @param initialState optional initial values that will be exposed as {@code state.initial}
   *     during execution. When {@code null}, the engine will look for a pre-defined initial state
   *     at {@code plan.state.initial}. If both are provided, the explicit {@code initialState}
   *     argument takes precedence and fully overrides any value from the plan.
   * @return the final output object produced by the {@code summary} node.
   * @throws ExecutionPlanException if validation fails, a node is malformed, an operation fails, or
   *     routing/json evaluation errors occur.
   */
  public JsonNode execute(JsonNode plan, JsonNode initialState) {
    // Validate structure before attempting to execute
    ExecutionPlanValidator.validate(plan);
    // Only the latest specification (no 'nodes' property) is supported now.
    return executeNewSpec(plan);
  }

  /**
   * Build an evaluation context for JSONPath resolver where each node is exposed at the root under
   * its node id. For backward compatibility, also exposes {@code _gstt} (the full state) and {@code
   * _lstt} (the last node result). While callers should prefer {@code $.<nodeId>} paths, the legacy
   * prefixes remain available so existing plans/tests continue to work.
   */
  private ObjectNode makeCtx(ObjectNode gstt, JsonNode lstt) {
    ObjectNode ctx = JsonNodeFactory.instance.objectNode();
    // Expose each node directly at root to support paths like $.node.var
    for (Iterator<Map.Entry<String, JsonNode>> it = gstt.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      ctx.set(e.getKey(), e.getValue());
    }
    // Back-compat aliases
    ctx.set("_gstt", gstt);
    if (lstt != null) ctx.set("_lstt", lstt);
    return ctx;
  }

  /**
   * Deeply resolve a JSON specification by evaluating any textual values that start with '$' as
   * JsonPath against the composed context (root exposes each node by its id). This method recurses
   * into objects and arrays and returns a new tree with all eligible values resolved.
   */
  private JsonNode deepResolve(JsonNode spec, ObjectNode gstt, JsonNode lstt) {
    if (spec == null || spec.isNull()) {
      return JsonNodeFactory.instance.nullNode();
    }
    ObjectNode ctx = makeCtx(gstt, lstt);
    if (spec.isTextual()) {
      String s = spec.asText();
      if (s.startsWith("$")) {
        return jsonPath.read(ctx, s);
      }
      // literal string, keep as-is
      return spec;
    }
    if (spec.isArray()) {
      ArrayNode out = JsonNodeFactory.instance.arrayNode();
      for (JsonNode el : spec) {
        JsonNode resolved = deepResolve(el, gstt, lstt);
        out.add(resolved);
      }
      return out;
    }
    if (spec.isObject()) {
      ObjectNode out = JsonNodeFactory.instance.objectNode();
      for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) spec).fields();
          it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        JsonNode v = e.getValue();
        JsonNode resolved = deepResolve(v, gstt, lstt);
        out.set(e.getKey(), resolved);
      }
      return out;
    }
    // numbers, booleans, etc.
    return spec;
  }

  /**
   * Resolve a node's {@code input} specification.
   *
   * <ul>
   *   <li>When {@code spec} is a string, it is treated as a JSONPath to be evaluated against the
   *       current context whose root exposes prior node results by their ids.
   *   <li>When {@code spec} is an object/array, any textual value starting with {@code $} is
   *       resolved via JSONPath recursively; other values are copied as-is.
   *   <li>Null/absent input resolves to an empty object.
   * </ul>
   */
  private JsonNode resolveInput(JsonNode spec, ObjectNode gstt, JsonNode lstt) {
    if (spec == null || spec.isNull()) {
      return JsonNodeFactory.instance.objectNode();
    }
    // Deep-resolve throughout objects and arrays
    return deepResolve(spec, gstt, lstt);
  }

  // ---------------- New-spec execution ----------------
  private JsonNode executeNewSpec(JsonNode plan) {
    ObjectNode gstt = JsonNodeFactory.instance.objectNode();

    // Initialize start_node vars
    JsonNode start = plan.get("start_node");
    ObjectNode startVars = JsonNodeFactory.instance.objectNode();
    JsonNode vars = start.get("vars");
    if (vars != null && vars.isObject()) {
      // deep resolve any JsonPath values (supports nested objects/arrays)
      JsonNode resolved = deepResolve(vars, gstt, null);
      if (resolved != null && resolved.isObject()) {
        startVars = (ObjectNode) resolved;
      }
    }
    gstt.set("start_node", startVars);

    String currentId = resolveRouteNew(plan, start.get("route"), gstt, startVars);
    while (true) {
      if (currentId == null) {
        throw new ExecutionPlanException("Routing resulted in null next node");
      }
      JsonNode nodeDef = plan.get(currentId);
      if (nodeDef == null || !nodeDef.isObject()) {
        throw new ExecutionPlanException("Node '" + currentId + "' not found");
      }
      // Terminal node
      if (nodeDef.has("completed") && nodeDef.get("completed").asBoolean(false)) {
        JsonNode outVars = nodeDef.get("vars");
        if (outVars == null || outVars.isNull()) {
          return JsonNodeFactory.instance.objectNode();
        }
        return resolveVars(outVars, gstt, null);
      }

      // HTTP call node (new structured format)
      if (nodeDef.has("http")) {
        String opName = nodeDef.get("operation").asText();
        JsonNode httpSpec = nodeDef.get("http");
        JsonNode http = resolveInput(httpSpec, gstt, null);
        JsonNode result = registry.invokeHttp(opName, http);
        if (result == null) result = JsonNodeFactory.instance.nullNode();
        gstt.set(currentId, result);
        currentId = resolveRouteNew(plan, nodeDef.get("route"), gstt, result);
        continue;
      }

      // Operation node (legacy format with flat input)
      String opName = nodeDef.get("operation").asText();
      JsonNode inputSpec = nodeDef.get("input");
      JsonNode input = resolveInput(inputSpec, gstt, null);
      JsonNode result = registry.invoke(opName, input);
      if (result == null) result = JsonNodeFactory.instance.nullNode();
      // store result under node id
      gstt.set(currentId, result);

      // compute next
      currentId = resolveRouteNew(plan, nodeDef.get("route"), gstt, result);
    }
  }

  private ObjectNode resolveVars(JsonNode varsSpec, ObjectNode gstt, JsonNode lstt) {
    if (varsSpec == null || varsSpec.isNull()) {
      return JsonNodeFactory.instance.objectNode();
    }
    if (!varsSpec.isObject()) {
      throw new ExecutionPlanException("Expected 'vars' to be an object");
    }
    JsonNode resolved = deepResolve(varsSpec, gstt, lstt);
    if (resolved != null && resolved.isObject()) {
      return (ObjectNode) resolved;
    }
    throw new ExecutionPlanException("Failed to resolve 'vars' as object");
  }

  private String resolveRouteNew(JsonNode plan, JsonNode route, ObjectNode gstt, JsonNode lstt) {
    if (route == null || route.isNull()) return null;
    if (route.isTextual()) return route.asText();
    if (route.isObject()) {
      // object-form: must contain exactly one entry whose value is a textual node id
      int fields = 0;
      String target = null;
      for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) route).fields();
          it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        fields++;
        if (fields > 1) break;
        JsonNode v = e.getValue();
        if (v != null && v.isTextual()) {
          target = v.asText();
        }
      }
      if (fields != 1 || target == null) {
        throw new ExecutionPlanException(
            "Route object form must contain exactly one entry with textual node id value");
      }
      return target;
    }
    if (!route.isArray()) {
      throw new ExecutionPlanException("Route must be string, object(single entry) or array");
    }
    int sz = route.size();
    if (sz == 0) return null;
    ObjectNode ctx = makeCtx(gstt, lstt);
    // Evaluate condition entries; last element is fallback string
    for (int i = 0; i < sz - 1; i++) {
      JsonNode entry = route.get(i);
      if (!entry.isObject()) continue;
      String cond =
          entry.has("condition") && entry.get("condition").isTextual()
              ? entry.get("condition").asText()
              : null;
      String node =
          entry.has("node") && entry.get("node").isTextual() ? entry.get("node").asText() : null;
      if (cond == null || node == null) continue;
      JsonNode val = jsonPath.read(ctx, cond);
      if (asBoolean(val)) {
        return node;
      }
    }
    JsonNode fallback = route.get(sz - 1);
    if (!fallback.isTextual()) {
      throw new ExecutionPlanException("Route array must end with fallback node id string");
    }
    return fallback.asText();
  }

  private boolean asBoolean(JsonNode v) {
    if (v == null || v.isNull()) return false;
    if (v.isBoolean()) return v.asBoolean();
    if (v.isNumber()) return v.asDouble() != 0.0;
    if (v.isTextual()) return !v.asText().isEmpty();
    if (v.isArray()) return v.size() > 0;
    if (v.isObject()) return v.size() > 0;
    return false;
  }

  /**
   * Project a node's local result and/or context into the standard state shape to store in {@code
   * _gstt.<nodeId>}.
   *
   * <p>Projection rules:
   *
   * <ul>
   *   <li>If {@code outputSpec} is null: when {@code lstt} is an object, copy its fields; else put
   *       the value under {@code value} key.
   *   <li>If {@code outputSpec} is a string: resolve as JSONPath; copy object results or assign to
   *       {@code value}.
   *   <li>If {@code outputSpec} is an object: resolve each textual value that starts with {@code $}
   *       and copy literals/structures as-is.
   * </ul>
   */
  private JsonNode projectOutput(
      String nodeId, JsonNode outputSpec, ObjectNode gstt, JsonNode lstt) {
    ObjectNode ctx = makeCtx(gstt, lstt);
    // Default: if output is absent, pass-through local state as-is
    if (outputSpec == null || outputSpec.isNull()) {
      return lstt == null ? JsonNodeFactory.instance.nullNode() : lstt;
    }
    // String: resolve and return value directly (object/array/primitive/null)
    if (outputSpec.isTextual()) {
      JsonNode resolved = jsonPath.read(ctx, outputSpec.asText());
      return resolved == null ? JsonNodeFactory.instance.nullNode() : resolved;
    }
    // Object: construct an object shape, resolving $-prefixed strings; copy literals as-is
    if (outputSpec.isObject()) {
      ObjectNode out = JsonNodeFactory.instance.objectNode();
      for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) outputSpec).fields();
          it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        JsonNode v = e.getValue();
        if (v.isTextual() && v.asText().startsWith("$")) {
          out.set(e.getKey(), jsonPath.read(ctx, v.asText()));
        } else {
          out.set(e.getKey(), v);
        }
      }
      return out;
    }
    throw new ExecutionPlanException("Node '" + nodeId + "' has invalid 'output' specification");
  }

  /**
   * Execute a {@code call} node: resolve input, invoke registry operation, project output, route.
   */
  private String executeCallNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
    String opName = nodeDef.path("operation").asText(null);
    if (opName == null) {
      throw new ExecutionPlanException("call node '%s' missing 'operation'".formatted(nodeId));
    }
    JsonNode result = registry.invoke(opName, input);
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, result);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Execute a {@code map} node: iterate input array/object, execute a single inner node for each
   * element, and collect outputs into an array stored under {@code value}.
   */
  private String executeMapNode(
      String nodeId, JsonNode nodeDef, ObjectNode gstt, ObjectNode nodes) {
    JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
    ArrayNode outArr = JsonNodeFactory.instance.arrayNode();
    String foreachNodeId = nodeDef.path("foreach").path("node").asText();
    if (input != null && input.isArray()) {
      int i = 0;
      for (JsonNode item : input) {
        ObjectNode kv = JsonNodeFactory.instance.objectNode();
        kv.put("key", i);
        kv.set("value", item);
        gstt.set(nodeId, kv); // expose for inner node via $._gstt.<mapNodeId>
        // Execute only the foreach node itself (no routing) to transform
        executeSingleNode(foreachNodeId, nodes.get(foreachNodeId), gstt);
        JsonNode produced = gstt.get(foreachNodeId);
        outArr.add(produced == null ? JsonNodeFactory.instance.nullNode() : produced);
        i++;
      }
    } else if (input != null && input.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) input).fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        ObjectNode kv = JsonNodeFactory.instance.objectNode();
        kv.put("key", e.getKey());
        kv.set("value", e.getValue());
        gstt.set(nodeId, kv); // key/value pair for current element
        executeSingleNode(foreachNodeId, nodes.get(foreachNodeId), gstt);
        JsonNode produced = gstt.get(foreachNodeId);
        outArr.add(produced == null ? JsonNodeFactory.instance.nullNode() : produced);
      }
    } else {
      throw new ExecutionPlanException("map node '" + nodeId + "' input is not a collection");
    }
    // Store the collection as local output for this node
    JsonNode lstt = outArr;
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Execute a single node without following its {@code next}. Used by {@code map.foreach} to run
   * the inner transformation node in isolation. Supported types: {@code call}, {@code expr}.
   */
  private void executeSingleNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    if (nodeDef == null || !nodeDef.isObject()) {
      throw new ExecutionPlanException("Node '" + nodeId + "' not found for map.foreach");
    }
    String type = nodeDef.path("type").asText();
    switch (type) {
      case "call" -> {
        JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
        String opName = nodeDef.path("operation").asText(null);
        if (opName == null)
          throw new ExecutionPlanException("call node '" + nodeId + "' missing 'operation'");
        JsonNode result = registry.invoke(opName, input);
        JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, result);
        gstt.set(nodeId, projected);
      }
      case "expr" -> {
        ObjectNode lstt = evalExprList(nodeId, nodeDef.get("expr"), gstt, null);
        JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
        gstt.set(nodeId, projected);
      }
      case "sort" -> {
        // Support sort as a foreach transformation without routing
        JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
        if (input == null || !input.isArray()) {
          throw new ExecutionPlanException("sort node '" + nodeId + "' expects array input");
        }
        String expr = nodeDef.path("expr").asText();
        String dir = nodeDef.path("dir").asText("asc");
        ArrayNode arr = (ArrayNode) input.deepCopy();
        java.util.List<JsonNode> list = new java.util.ArrayList<>();
        arr.forEach(list::add);
        list.sort(
            (a, b) -> {
              JsonNode ka = jsonPath.read(a, expr);
              JsonNode kb = jsonPath.read(b, expr);
              int cmp;
              if (ka == null || ka.isNull()) {
                cmp = (kb == null || kb.isNull()) ? 0 : 1;
              } else if (kb == null || kb.isNull()) {
                cmp = -1;
              } else if (ka.isNumber() && kb.isNumber()) {
                cmp = Double.compare(ka.asDouble(), kb.asDouble());
              } else {
                cmp = ka.asText().compareTo(kb.asText());
              }
              return "desc".equalsIgnoreCase(dir) ? -cmp : cmp;
            });
        ArrayNode out = JsonNodeFactory.instance.arrayNode();
        list.forEach(out::add);
        JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, out);
        gstt.set(nodeId, projected);
      }
      default -> throw new ExecutionPlanException("Unsupported foreach node type '" + type + "'");
    }
  }

  /** Sort an array input based on a JSONPath key selector and direction. */
  private String executeSortNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
    if (input == null || !input.isArray()) {
      throw new ExecutionPlanException("sort node '" + nodeId + "' expects array input");
    }
    String expr = nodeDef.path("expr").asText();
    String dir = nodeDef.path("dir").asText("asc");
    ArrayNode arr = (ArrayNode) input.deepCopy();
    java.util.List<JsonNode> list = new java.util.ArrayList<>();
    arr.forEach(list::add);
    list.sort(
        (a, b) -> {
          JsonNode ka = jsonPath.read(a, expr); // root is item
          JsonNode kb = jsonPath.read(b, expr);
          int cmp;
          if (ka == null || ka.isNull()) {
            cmp = (kb == null || kb.isNull()) ? 0 : 1; // nulls last
          } else if (kb == null || kb.isNull()) {
            cmp = -1;
          } else if (ka.isNumber() && kb.isNumber()) {
            cmp = Double.compare(ka.asDouble(), kb.asDouble());
          } else {
            cmp = ka.asText().compareTo(kb.asText());
          }
          return "desc".equalsIgnoreCase(dir) ? -cmp : cmp;
        });
    ArrayNode out = JsonNodeFactory.instance.arrayNode();
    list.forEach(out::add);
    JsonNode lstt = out;
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Transform each input item into a key/value pair. Keys and values are built by evaluating the
   * provided JSONPath expressions against the item root.
   */
  private String executeGroupNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
    if (input == null || !input.isArray()) {
      log.error(
          "group node '{}' expects array input.\n{}",
          nodeId,
          StringUtility.formatWithIndent(JacksonUtility.toJson(input), 4));
      throw new ExecutionPlanException("group node '" + nodeId + "' expects array input");
    }
    // New semantics: build an object where each field name is a stable stringified key object,
    // and each field value is an array of value objects for that key.
    ObjectNode grouped = JsonNodeFactory.instance.objectNode();
    ArrayNode keyExprs = (ArrayNode) nodeDef.get("key");
    ArrayNode valExprs =
        nodeDef.has("value") && nodeDef.get("value").isArray()
            ? (ArrayNode) nodeDef.get("value")
            : null;
    for (JsonNode item : input) {
      // Build key object
      ObjectNode keyObj = JsonNodeFactory.instance.objectNode();
      int i = 0;
      for (JsonNode k : keyExprs) {
        JsonNode kv = jsonPath.read(item, k.asText());
        keyObj.set("k" + i, kv == null ? JsonNodeFactory.instance.nullNode() : kv);
        i++;
      }
      // Create deterministic string key
      String keyStr;
      try {
        keyStr =
            com.fasterxml.jackson.databind.json.JsonMapper.builder()
                .build()
                .writeValueAsString(keyObj);
      } catch (Exception e) {
        keyStr = keyObj.toString();
      }
      // Build value object
      ObjectNode valObj = JsonNodeFactory.instance.objectNode();
      if (valExprs != null) {
        int j = 0;
        for (JsonNode ve : valExprs) {
          JsonNode vv = jsonPath.read(item, ve.asText());
          valObj.set("v" + j, vv == null ? JsonNodeFactory.instance.nullNode() : vv);
          j++;
        }
      } else {
        // Whole item becomes the value
        valObj.set("value", item);
      }
      // Append to group array
      ArrayNode arr =
          grouped.has(keyStr) && grouped.get(keyStr).isArray()
              ? (ArrayNode) grouped.get(keyStr)
              : JsonNodeFactory.instance.arrayNode();
      arr.add(valObj);
      grouped.set(keyStr, arr);
    }
    JsonNode lstt = grouped;
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Keep only the items for which the {@code expr} evaluates to {@code true}. String expressions
   * may use compact boolean predicate syntax and are delegated to {@link FilterPredicateEvaluator}
   * when necessary.
   */
  private String executeFilterNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    JsonNode input = resolveInput(nodeDef.get("input"), gstt, null);
    if (input == null || !input.isArray()) {
      log.error(
          "group node '{}' expects array input.\n{}",
          nodeId,
          StringUtility.formatWithIndent(JacksonUtility.toJson(input), 4));
      throw new ExecutionPlanException("filter node '" + nodeId + "' expects array input");
    }
    JsonNode expr = nodeDef.get("expr");
    ArrayNode out = JsonNodeFactory.instance.arrayNode();
    for (JsonNode item : input) {
      if (evaluateCondition(expr, item)) {
        out.add(item);
      }
    }
    JsonNode lstt = out;
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Evaluate a filter condition against a single item. Supports:
   *
   * <ul>
   *   <li>String JSONPath returning a boolean
   *   <li>Compact predicate syntax (e.g., {@code @.age >= 18 && @.active})
   *   <li>Object form with {@code and}/{@code or} keys containing nested expressions
   * </ul>
   */
  private boolean evaluateCondition(JsonNode expr, JsonNode itemRoot) {
    if (expr == null || expr.isNull()) return false;
    if (expr.isTextual()) {
      String s = expr.asText();
      // Detect complex predicate syntax and evaluate via FilterPredicateEvaluator
      if (s.contains("&&")
          || s.contains("||")
          || s.startsWith("$[?(")
          || s.startsWith("@")
          || s.contains("@.")) {
        try {
          return FilterPredicateEvaluator.evaluate(s, itemRoot, jsonPath);
        } catch (Exception e) {
          throw new ExecutionPlanException("Failed to evaluate filter predicate: " + s, e);
        }
      }
      JsonNode r = jsonPath.read(itemRoot, s);
      return r != null && r.isBoolean() && r.asBoolean();
    }
    if (expr.isObject()) {
      if (expr.has("and")) {
        JsonNode andObj = expr.get("and");
        for (Iterator<Map.Entry<String, JsonNode>> it = andObj.fields(); it.hasNext(); ) {
          JsonNode v = it.next().getValue();
          if (!evaluateCondition(v, itemRoot)) return false;
        }
        return true;
      }
      if (expr.has("or")) {
        JsonNode orObj = expr.get("or");
        for (Iterator<Map.Entry<String, JsonNode>> it = orObj.fields(); it.hasNext(); ) {
          JsonNode v = it.next().getValue();
          if (evaluateCondition(v, itemRoot)) return true;
        }
        return false;
      }
    }
    return false;
  }

  /** Build a string by conditionally appending literals or evaluated expressions. */
  private String executeConcatNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    ArrayNode parts = (ArrayNode) nodeDef.get("concat");
    StringBuilder sb = new StringBuilder();
    int appended = 0;
    ObjectNode ctx = makeCtx(gstt, null);
    for (JsonNode p : parts) {
      boolean include = true;
      if (p.has("condition")) {
        JsonNode r = jsonPath.read(ctx, p.get("condition").asText());
        include = r != null && r.isBoolean() && r.asBoolean();
      }
      if (!include) continue;
      if (p.has("literal")) {
        sb.append(p.get("literal").asText());
        appended++;
      } else if (p.has("expr")) {
        JsonNode r = jsonPath.read(ctx, p.get("expr").asText());
        sb.append(r == null || r.isNull() ? "" : r.asText());
        appended++;
      }
    }
    if (appended == 0) {
      throw new ExecutionPlanException(
          "concat node '" + nodeId + "' produced empty content after conditions");
    }
    JsonNode lstt = JsonNodeFactory.instance.textNode(sb.toString());
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Perform stepwise combination of arrays referenced by JSONPath expressions.
   *
   * <p>Each step specifies {@code alias}, {@code left}, optional {@code right}, and an optional
   * join-like {@code condition}. Left/right aliases are derived from the first path segment of
   * those expressions and are used to prefix merged fields to avoid collisions.
   */
  private String executeCombineNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    ObjectNode input = (ObjectNode) resolveInput(nodeDef.get("input"), gstt, null);
    ObjectNode lstt = JsonNodeFactory.instance.objectNode();
    ArrayNode steps = (ArrayNode) nodeDef.get("combine");
    ArrayNode lastResult = null;
    for (JsonNode step : steps) {
      String alias = step.path("alias").asText();
      String leftPath = step.path("left").asText();
      String rightPath = step.path("right").asText(null);
      // Build context with available aliases
      ObjectNode ctx = makeCtx(gstt, lstt);
      ctx.setAll(input);
      JsonNode left = jsonPath.read(ctx, leftPath);
      JsonNode right = rightPath == null ? null : jsonPath.read(ctx, rightPath);
      ArrayNode leftArr = asArray(left);
      ArrayNode rightArr = right == null ? null : asArray(right);
      ArrayNode combined = JsonNodeFactory.instance.arrayNode();
      String leftAlias = resolveAliasFromPath(leftPath);
      String rightAlias = rightPath == null ? null : resolveAliasFromPath(rightPath);
      if (rightArr != null) {
        for (JsonNode li : leftArr) {
          boolean matched = false;
          for (JsonNode ri : rightArr) {
            if (matchPair(step.get("condition"), li, ri, gstt, lstt)) {
              combined.add(mergePrefixed(li, leftAlias, ri, rightAlias));
              matched = true;
            }
          }
          if (!matched) {
            combined.add(mergePrefixed(li, leftAlias, null, rightAlias));
          }
        }
      } else {
        for (JsonNode li : leftArr) {
          combined.add(mergePrefixed(li, leftAlias, null, null));
        }
      }
      lstt.set(alias, combined);
      lastResult = combined;
    }
    if (lastResult == null) lastResult = JsonNodeFactory.instance.arrayNode();
    JsonNode lsttOut = lastResult;
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lsttOut);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /** Normalize a value to an array: {@code null -> []}, {@code value -> [value]}. */
  private ArrayNode asArray(JsonNode n) {
    ArrayNode arr = JsonNodeFactory.instance.arrayNode();
    if (n == null || n.isNull()) return arr;
    if (n.isArray()) {
      n.forEach(arr::add);
      return arr;
    }
    arr.add(n);
    return arr;
  }

  /**
   * Derive a best-effort alias from a JSONPath: strips {@code $._lstt.}, {@code $._gstt.}, or
   * leading {@code $.} and returns the first path segment.
   */
  private String resolveAliasFromPath(String path) {
    // best-effort: if path starts with $._lstt.alias or $._gstt.alias or $.inputKey
    String p = path;
    if (p.startsWith("$._lstt.")) {
      p = p.substring("$._lstt.".length());
    } else if (p.startsWith("$._gstt.")) {
      p = p.substring("$._gstt.".length());
    } else if (p.startsWith("$.")) {
      p = p.substring(2);
    }
    int dot = p.indexOf('.');
    return dot > 0 ? p.substring(0, dot) : p;
  }

  /**
   * Evaluate a join condition for pair (left,right). Supports {@code and}/{@code or} objects with
   * comparison operators as keys and two-argument arrays as values.
   */
  private boolean matchPair(
      JsonNode condition, JsonNode left, JsonNode right, ObjectNode gstt, ObjectNode lstt) {
    if (condition == null || condition.isNull()) return true;
    if (!condition.isObject()) return false;
    ObjectNode ctx = JsonNodeFactory.instance.objectNode();
    ctx.set("_left", left);
    ctx.set("_right", right);
    ctx.set("_gstt", gstt);
    ctx.set("_lstt", lstt);
    // condition like {"and": {"==": ["$._left.id", "$._right.customer_id"]}}
    if (condition.has("and")) {
      JsonNode obj = condition.get("and");
      for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) obj).fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        if (!evaluateComparison(e.getKey(), e.getValue(), ctx)) return false;
      }
      return true;
    }
    if (condition.has("or")) {
      JsonNode obj = condition.get("or");
      for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) obj).fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        if (evaluateComparison(e.getKey(), e.getValue(), ctx)) return true;
      }
      return false;
    }
    return false;
  }

  /** Evaluate a binary comparison like {@code {"==": ["$._left.id", "$._right.customer_id"]}}. */
  private boolean evaluateComparison(String op, JsonNode arr, ObjectNode ctx) {
    if (!(arr != null && arr.isArray() && arr.size() == 2)) return false;
    JsonNode a = jsonPath.read(ctx, arr.get(0).asText());
    JsonNode b = jsonPath.read(ctx, arr.get(1).asText());
    switch (op) {
      case "==":
        return java.util.Objects.equals(asComparable(a), asComparable(b));
      case "!=":
        return !java.util.Objects.equals(asComparable(a), asComparable(b));
      case "<":
        return compareAsDouble(a, b) < 0;
      case "<=":
        return compareAsDouble(a, b) <= 0;
      case ">":
        return compareAsDouble(a, b) > 0;
      case ">=":
        return compareAsDouble(a, b) >= 0;
      default:
        return false;
    }
  }

  /** Coerce a {@link JsonNode} to a Java comparable value for equality checks. */
  private Object asComparable(JsonNode n) {
    if (n == null || n.isNull()) return null;
    if (n.isNumber()) return n.asDouble();
    if (n.isBoolean()) return n.asBoolean();
    return n.asText();
  }

  /** Compare two nodes as doubles with {@code NaN} ordering semantics (NaN last). */
  private int compareAsDouble(JsonNode a, JsonNode b) {
    double da = a == null || a.isNull() ? Double.NaN : a.asDouble();
    double db = b == null || b.isNull() ? Double.NaN : b.asDouble();
    if (Double.isNaN(da) && Double.isNaN(db)) return 0;
    if (Double.isNaN(da)) return 1;
    if (Double.isNaN(db)) return -1;
    return Double.compare(da, db);
  }

  /**
   * Merge two objects, prefixing each field name with its alias (e.g., {@code a.id}, {@code b.id}).
   */
  private ObjectNode mergePrefixed(
      JsonNode left, String leftAlias, JsonNode right, String rightAlias) {
    ObjectNode out = JsonNodeFactory.instance.objectNode();
    if (left != null && left.isObject()) {
      ((ObjectNode) left)
          .fields()
          .forEachRemaining(e -> out.set(leftAlias + "." + e.getKey(), e.getValue()));
    }
    if (right != null && right.isObject()) {
      ((ObjectNode) right)
          .fields()
          .forEachRemaining(e -> out.set(rightAlias + "." + e.getKey(), e.getValue()));
    }
    return out;
  }

  /** Execute an {@code expr} node: compute numeric expressions and project output. */
  private String executeExprNode(String nodeId, JsonNode nodeDef, ObjectNode gstt) {
    ObjectNode lstt = evalExprList(nodeId, nodeDef.get("expr"), gstt, null);
    JsonNode projected = projectOutput(nodeId, nodeDef.get("output"), gstt, lstt);
    gstt.set(nodeId, projected);
    return resolveNextNode(nodeId, nodeDef.get("next"), gstt, projected);
  }

  /**
   * Evaluate a list of arithmetic expressions, each storing its result under an {@code alias}.
   * Supported operators: {@code +, -, *, /, %, min, max}.
   */
  private ObjectNode evalExprList(
      String nodeId, JsonNode exprList, ObjectNode gstt, JsonNode baseLstt) {
    ObjectNode lstt =
        baseLstt != null && baseLstt.isObject()
            ? (ObjectNode) baseLstt
            : JsonNodeFactory.instance.objectNode();
    if (exprList != null && exprList.isArray()) {
      for (JsonNode e : exprList) {
        String alias = e.path("alias").asText(null);
        JsonNode opSpec = null;
        String op = null;
        for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) e).fields(); it.hasNext(); ) {
          Map.Entry<String, JsonNode> entry = it.next();
          if (!"alias".equals(entry.getKey())) {
            op = entry.getKey();
            opSpec = entry.getValue();
            break;
          }
        }
        if (alias == null || op == null || opSpec == null || !opSpec.isArray()) {
          throw new ExecutionPlanException(
              "expr node '" + nodeId + "' has invalid expression entry");
        }
        double result;
        switch (op) {
          case "+":
            result = foldNumbers(opSpec, gstt, lstt, 0.0, (a, b) -> a + b, false);
            break;
          case "-":
            result = foldNumbers(opSpec, gstt, lstt, null, (a, b) -> a - b, true);
            break;
          case "*":
            result = foldNumbers(opSpec, gstt, lstt, 1.0, (a, b) -> a * b, false);
            break;
          case "/":
            result = foldNumbers(opSpec, gstt, lstt, null, (a, b) -> a / b, true);
            break;
          case "%":
            result = foldNumbers(opSpec, gstt, lstt, null, (a, b) -> a % b, true);
            break;
          case "min":
            result = foldNumbers(opSpec, gstt, lstt, null, Math::min, true);
            break;
          case "max":
            result = foldNumbers(opSpec, gstt, lstt, null, Math::max, true);
            break;
          default:
            throw new ExecutionPlanException(
                "expr node '" + nodeId + "' unsupported operator '" + op + "'");
        }
        lstt.put(alias, result);
      }
    }
    return lstt;
  }

  /** Small functional interface for numeric folding. */
  private interface DoubleOp {
    double apply(double a, double b);
  }

  /**
   * Fold a sequence of numbers resolved from JSONPath strings or numeric literals.
   *
   * @param seed initial accumulator value; when {@code null}, the first element is used.
   * @param firstAsSeed if true, treat the first element as the initial accumulator regardless of
   *     seed; useful for non-commutative operations.
   */
  private double foldNumbers(
      JsonNode arr, ObjectNode gstt, JsonNode lstt, Double seed, DoubleOp op, boolean firstAsSeed) {
    double acc = seed != null ? seed : 0.0;
    boolean first = true;
    ObjectNode ctx = makeCtx(gstt, lstt);
    for (JsonNode v : arr) {
      double d;
      if (v.isTextual()) {
        JsonNode r = jsonPath.read(ctx, v.asText());
        d = r == null || r.isNull() ? 0d : r.asDouble();
      } else if (v.isNumber()) {
        d = v.asDouble();
      } else {
        d = 0d;
      }
      if (first) {
        if (firstAsSeed || seed == null) {
          acc = d;
          first = false;
          continue;
        }
        first = false;
      }
      acc = op.apply(acc, d);
    }
    return acc;
  }

  /** Build the final summary object by resolving expressions against combined context. */
  private JsonNode executeSummaryNode(JsonNode nodeDef, ObjectNode gstt) {
    ObjectNode ctx = makeCtx(gstt, gstt);
    JsonNode outputSpec = nodeDef.get("output");
    if (outputSpec == null || !outputSpec.isObject()) {
      return JsonNodeFactory.instance.objectNode();
    }
    ObjectNode out = JsonNodeFactory.instance.objectNode();
    for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) outputSpec).fields();
        it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      JsonNode v = e.getValue();
      if (v.isTextual() && v.asText().startsWith("$")) {
        out.set(e.getKey(), jsonPath.read(ctx, v.asText()));
      } else {
        out.set(e.getKey(), v);
      }
    }
    return out;
  }

  /**
   * Resolve the next node id given a {@code next} specification.
   *
   * <ul>
   *   <li>{@code null}: end of plan
   *   <li>string: direct target
   *   <li>array: first route whose {@code expr} resolves to true; otherwise fallback to the first
   *       route without {@code expr}
   * </ul>
   */
  private String resolveNextNode(String nodeId, JsonNode next, ObjectNode gstt, JsonNode lstt) {
    if (next == null || next.isNull()) {
      return null; // end of plan
    }
    if (next.isTextual()) {
      return next.asText();
    }
    if (next.isArray()) {
      ObjectNode ctx = makeCtx(gstt, lstt);
      String fallback = null;
      for (JsonNode route : next) {
        String target = route.path("node").asText(null);
        if (target == null) continue;
        if (!route.has("expr")) {
          fallback = target;
          continue;
        }
        JsonNode r = jsonPath.read(ctx, route.get("expr").asText());
        if (r != null && r.isBoolean() && r.asBoolean()) {
          return target;
        }
      }
      return fallback;
    }
    throw new ExecutionPlanException("Invalid 'next' specification for node '" + nodeId + "'");
  }
}
