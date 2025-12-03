package com.gentoro.onemcp.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.gentoro.onemcp.cache.FilterExpression;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.PromptSchemaShape;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.Iterator;
import java.util.List;
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

  /** Pattern to match all placeholders: {{params.X}}, {{filter.X.value}}, {{shape.X}} */
  private static final Pattern PLACEHOLDER_PATTERN = 
      Pattern.compile("\\{\\{(params|filter|shape)\\.([a-zA-Z0-9_.]+)\\}\\}");

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

    // 1. Hydrate placeholders with values from schema (params, filter, shape)
    JsonNode hydratedPlan = hydrate(planNode, schema);
    log.trace("Hydrated plan: {}", hydratedPlan);

    // 2. Execute via engine
    ExecutionPlanEngine engine = new ExecutionPlanEngine(
        JacksonUtility.getJsonMapper(), registry);
    JsonNode result = engine.execute(hydratedPlan, null);
    log.debug("Execution result: {}", result);
    
    if (result == null) {
      log.error("ExecutionPlanEngine returned null result for schema: {}", schema.getCacheKey());
      throw new RuntimeException("Plan execution returned null result");
    }

    // 2.5. Check for API failures (success: false in response)
    // This can happen when the API call succeeds but returns an error response
    if (result.isObject()) {
      JsonNode successNode = result.get("success");
      if (successNode != null && successNode.isBoolean() && !successNode.asBoolean()) {
        // API returned success: false
        String errorMsg = "API call failed";
        JsonNode errorNode = result.get("error");
        if (errorNode != null && errorNode.isTextual()) {
          errorMsg = "API error: " + errorNode.asText();
        } else if (errorNode != null) {
          errorMsg = "API error: " + errorNode.toString();
        }
        log.warn("API returned success: false for schema: {}. Error: {}", schema.getCacheKey(), errorMsg);
        throw new RuntimeException(errorMsg);
      }
    }

    // 3. Format result
    String formatted = formatResult(result);
    if (formatted == null || formatted.trim().isEmpty()) {
      log.error("Formatted result is null or empty for schema: {}", schema.getCacheKey());
      log.debug("Raw result was: {}", result);
      throw new RuntimeException("Plan execution produced null or empty formatted result");
    }
    return formatted;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Hydration - Replace {{params.X}}, {{filter.X.value}}, {{shape.X}} with actual values
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Hydrate placeholders in the plan with actual values from schema.
   *
   * @param node the JSON node to hydrate
   * @param schema the PromptSchema containing params, filter, and shape values
   * @return a new JSON node with placeholders replaced
   */
  private JsonNode hydrate(JsonNode node, PromptSchema schema) {
    if (node == null || node.isNull()) {
      return node;
    }

    if (node.isTextual()) {
      String text = node.asText();
      return hydrateText(text, schema);
    }

    if (node.isArray()) {
      ArrayNode result = JacksonUtility.getJsonMapper().createArrayNode();
      for (JsonNode element : node) {
        result.add(hydrate(element, schema));
      }
      return result;
    }

    if (node.isObject()) {
      ObjectNode result = JacksonUtility.getJsonMapper().createObjectNode();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        result.set(field.getKey(), hydrate(field.getValue(), schema));
      }
      return result;
    }

    // Numbers, booleans, etc. - return as-is
    return node;
  }

  /**
   * Hydrate a text value, replacing placeholders with actual values.
   * Supports: {{params.X}}, {{filter.field_name.value}}, {{shape.limit}}, {{shape.offset}}
   *
   * @param text the text to hydrate
   * @param schema the PromptSchema containing all values
   * @return a JsonNode with the hydrated value
   */
  private JsonNode hydrateText(String text, PromptSchema schema) {
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(text);
    
    // If the entire text is a single placeholder, return the actual value type
    if (matcher.matches()) {
      String namespace = matcher.group(1); // params, filter, or shape
      String path = matcher.group(2);
      Object value = resolvePlaceholder(namespace, path, schema);
      return valueToJsonNode(value);
    }

    // If text contains placeholders mixed with other text, do string replacement
    StringBuffer sb = new StringBuffer();
    matcher.reset();
    while (matcher.find()) {
      String namespace = matcher.group(1);
      String path = matcher.group(2);
      Object value = resolvePlaceholder(namespace, path, schema);
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
   * Resolve a placeholder value from the schema.
   *
   * @param namespace "params", "filter", or "shape"
   * @param path the path within the namespace (e.g., "date_year.value" for filter)
   * @param schema the PromptSchema
   * @return the resolved value, or null if not found
   */
  private Object resolvePlaceholder(String namespace, String path, PromptSchema schema) {
    switch (namespace) {
      case "params":
        return resolveParamPath(path, schema.getParams());
        
      case "filter":
        return resolveFilterPath(path, schema.getFilter());
        
      case "shape":
        return resolveShapePath(path, schema.getShape());
        
      default:
        log.warn("Unknown placeholder namespace: {}", namespace);
        return null;
    }
  }

  /**
   * Resolve a filter path like "date_year.value" from the filter list.
   * Looks up filter by field name and returns the specified property.
   *
   * @param path the path (format: "field_name.property", e.g., "date_year.value")
   * @param filters the list of filter expressions
   * @return the resolved value, or null if not found
   */
  private Object resolveFilterPath(String path, List<FilterExpression> filters) {
    if (filters == null || filters.isEmpty() || path == null) {
      return null;
    }
    
    // Parse path: field_name.property (e.g., "date_year.value", "customer_state.value")
    int dotIndex = path.lastIndexOf('.');
    if (dotIndex < 0) {
      log.warn("Invalid filter path (expected field.property): {}", path);
      return null;
    }
    
    String fieldName = path.substring(0, dotIndex);
    String property = path.substring(dotIndex + 1);
    
    // Find filter by field name
    for (FilterExpression fe : filters) {
      if (fe != null && fieldName.equals(fe.getField())) {
        switch (property) {
          case "value":
            return fe.getValue();
          case "operator":
            return fe.getOperator();
          case "field":
            return fe.getField();
          default:
            log.warn("Unknown filter property: {}", property);
            return null;
        }
      }
    }
    
    log.warn("Filter not found for field: {}", fieldName);
    return null;
  }

  /**
   * Resolve a shape path like "limit" or "offset" from the shape object.
   *
   * @param path the property name (e.g., "limit", "offset")
   * @param shape the PromptSchemaShape
   * @return the resolved value, or null if not found
   */
  private Object resolveShapePath(String path, PromptSchemaShape shape) {
    if (shape == null || path == null) {
      return null;
    }
    
    switch (path) {
      case "limit":
        return shape.getLimit();
      case "offset":
        return shape.getOffset();
      default:
        log.warn("Unknown shape property: {}", path);
        return null;
    }
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

    StringBuilder output = new StringBuilder();
    
    // If result has an "answer" field, use it
    if (result.has("answer")) {
      JsonNode answer = result.get("answer");
      if (answer.isTextual()) {
        output.append(answer.asText());
      } else {
        // Try to format as JSON if it's an object/array
        try {
          output.append(JacksonUtility.getJsonMapper()
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(answer));
        } catch (Exception e) {
          output.append(answer.toString());
        }
      }
    }

    // If result has a "message" field, append it (for shaping warnings)
    if (result.has("message")) {
      JsonNode message = result.get("message");
      String messageStr = message.isTextual() ? message.asText() : message.toString();
      if (!messageStr.trim().isEmpty()) {
        if (output.length() > 0) {
          output.append("\n\n");
        }
        output.append(messageStr);
      }
    }

    // If we have output, return it
    if (output.length() > 0) {
      return output.toString();
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

