package com.gentoro.onemcp.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * First-class representation of an execution plan.
 *
 * <p>Encapsulates the JSON DAG structure. The JSON representation is an internal detail - 
 * callers interact with this object through its methods.
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

