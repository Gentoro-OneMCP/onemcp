package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * First-class representation of an execution plan.
 *
 * <p>Encapsulates the JSON DAG structure and knows how to execute itself.
 * The JSON representation is an internal detail - callers interact with
 * this object through its methods.
 *
 * <p>Plans may contain placeholders like {@code {{params.year}}} that are
 * hydrated with actual values from a {@link PromptSchema} at execution time.
 */
public class ExecutionPlan {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ExecutionPlan.class);

  /** Pattern to match placeholders: {{params.field}} or {{params.field.subfield}} */
  private static final Pattern PLACEHOLDER_PATTERN = 
      Pattern.compile("\\{\\{params\\.([a-zA-Z0-9_.]+)\\}\\}");

  private final JsonNode planNode;

  // ─────────────────────────────────────────────────────────────────────────
  // Construction
  // ─────────────────────────────────────────────────────────────────────────

  private ExecutionPlan(JsonNode planNode) {
    if (planNode == null) {
      throw new IllegalArgumentException("Plan node cannot be null");
    }
    this.planNode = planNode;
  }

  /**
   * Create an ExecutionPlan from a JSON string.
   *
   * @param json the JSON representation of the plan
   * @return the ExecutionPlan
   * @throws IllegalArgumentException if JSON is invalid
   */
  public static ExecutionPlan fromJson(String json) {
    if (json == null || json.trim().isEmpty()) {
      throw new IllegalArgumentException("JSON cannot be null or empty");
    }
    try {
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      JsonNode node = mapper.readTree(json);
      return new ExecutionPlan(node);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Create an ExecutionPlan from a JsonNode.
   *
   * @param node the JSON node representing the plan
   * @return the ExecutionPlan
   */
  public static ExecutionPlan fromNode(JsonNode node) {
    return new ExecutionPlan(node);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Serialization
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Serialize this plan to a JSON string.
   *
   * @return JSON string representation
   */
  public String toJson() {
    try {
      return JacksonUtility.getJsonMapper().writeValueAsString(planNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize plan to JSON", e);
    }
  }

  /**
   * Serialize this plan to a pretty-printed JSON string.
   *
   * @return pretty JSON string representation
   */
  public String toPrettyJson() {
    try {
      return JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(planNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize plan to JSON", e);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Execution
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Execute this plan with parameter values from the schema.
   *
   * <p>This method:
   * <ol>
   *   <li>Hydrates placeholders ({{params.X}}) with values from schema.params
   *   <li>Executes the hydrated plan using ExecutionPlanEngine
   *   <li>Formats and returns the result
   * </ol>
   *
   * @param schema the PromptSchema containing parameter values
   * @param registry the operation registry for executing operations
   * @return the execution result as a formatted string
   */
  public String execute(PromptSchema schema, OperationRegistry registry) {
    log.debug("Executing plan with schema: {}", schema.getCacheKey());

    // 1. Hydrate placeholders with values from schema.params
    JsonNode hydratedPlan = hydrate(planNode, schema.getParams());
    log.trace("Hydrated plan: {}", hydratedPlan);

    // 2. Execute via engine
    ExecutionPlanEngine engine = new ExecutionPlanEngine(
        JacksonUtility.getJsonMapper(), registry);
    JsonNode result = engine.execute(hydratedPlan, null);
    log.debug("Execution result: {}", result);

    // 3. Format result
    return formatResult(result);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Hydration - Replace {{params.X}} with actual values
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Hydrate placeholders in the plan with actual values.
   *
   * @param node the JSON node to hydrate
   * @param params the parameter values from PromptSchema
   * @return a new JSON node with placeholders replaced
   */
  private JsonNode hydrate(JsonNode node, Map<String, Object> params) {
    if (node == null || node.isNull()) {
      return node;
    }

    if (node.isTextual()) {
      String text = node.asText();
      return hydrateText(text, params);
    }

    if (node.isArray()) {
      ArrayNode result = JacksonUtility.getJsonMapper().createArrayNode();
      for (JsonNode element : node) {
        result.add(hydrate(element, params));
      }
      return result;
    }

    if (node.isObject()) {
      ObjectNode result = JacksonUtility.getJsonMapper().createObjectNode();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        result.set(field.getKey(), hydrate(field.getValue(), params));
      }
      return result;
    }

    // Numbers, booleans, etc. - return as-is
    return node;
  }

  /**
   * Hydrate a text value, replacing {{params.X}} placeholders.
   *
   * @param text the text to hydrate
   * @param params the parameter values
   * @return a JsonNode with the hydrated value
   */
  private JsonNode hydrateText(String text, Map<String, Object> params) {
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(text);
    
    // If the entire text is a single placeholder, return the actual value type
    if (matcher.matches()) {
      String path = matcher.group(1);
      Object value = resolveParamPath(path, params);
      return valueToJsonNode(value);
    }

    // If text contains placeholders mixed with other text, do string replacement
    StringBuffer sb = new StringBuffer();
    matcher.reset();
    while (matcher.find()) {
      String path = matcher.group(1);
      Object value = resolveParamPath(path, params);
      String replacement = value != null ? value.toString() : "";
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    
    String result = sb.toString();
    // If nothing changed, return original
    if (result.equals(text)) {
      return new TextNode(text);
    }
    return new TextNode(result);
  }

  /**
   * Resolve a dotted path like "amount.aggregate" from the params map.
   *
   * @param path the dotted path (e.g., "year" or "amount.aggregate")
   * @param params the parameter map
   * @return the resolved value, or null if not found
   */
  @SuppressWarnings("unchecked")
  private Object resolveParamPath(String path, Map<String, Object> params) {
    if (params == null || path == null || path.isEmpty()) {
      return null;
    }

    String[] parts = path.split("\\.");
    Object current = params;

    for (String part : parts) {
      if (current == null) {
        return null;
      }
      if (current instanceof Map) {
        current = ((Map<String, Object>) current).get(part);
      } else {
        return null;
      }
    }

    return current;
  }

  /**
   * Convert a Java value to a JsonNode.
   *
   * @param value the value to convert
   * @return the JsonNode representation
   */
  private JsonNode valueToJsonNode(Object value) {
    if (value == null) {
      return JacksonUtility.getJsonMapper().nullNode();
    }
    try {
      return JacksonUtility.getJsonMapper().valueToTree(value);
    } catch (Exception e) {
      // Fallback to string representation
      return new TextNode(value.toString());
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Result formatting
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Format the execution result into a user-friendly string.
   *
   * @param result the execution result JsonNode
   * @return formatted string
   */
  private String formatResult(JsonNode result) {
    if (result == null || result.isNull()) {
      return "Execution completed but no result was produced.";
    }

    // If result has an "answer" field, use it
    if (result.has("answer")) {
      JsonNode answer = result.get("answer");
      if (answer.isTextual()) {
        return answer.asText();
      }
      return answer.toString();
    }

    // If result has a "message" field, use it
    if (result.has("message")) {
      JsonNode message = result.get("message");
      if (message.isTextual()) {
        return message.asText();
      }
      return message.toString();
    }

    // Otherwise return the JSON representation
    try {
      return JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(result);
    } catch (Exception e) {
      return result.toString();
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Utility
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Check if this plan contains any placeholders.
   *
   * @return true if the plan has {{params.X}} placeholders
   */
  public boolean hasPlaceholders() {
    return containsPlaceholders(planNode);
  }

  private boolean containsPlaceholders(JsonNode node) {
    if (node == null || node.isNull()) {
      return false;
    }
    if (node.isTextual()) {
      return PLACEHOLDER_PATTERN.matcher(node.asText()).find();
    }
    if (node.isArray()) {
      for (JsonNode element : node) {
        if (containsPlaceholders(element)) {
          return true;
        }
      }
    }
    if (node.isObject()) {
      Iterator<JsonNode> elements = node.elements();
      while (elements.hasNext()) {
        if (containsPlaceholders(elements.next())) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "ExecutionPlan{hasPlaceholders=" + hasPlaceholders() + "}";
  }
}


