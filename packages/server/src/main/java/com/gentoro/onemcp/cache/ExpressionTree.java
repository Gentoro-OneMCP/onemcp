package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Expression tree node for WHERE clauses (cache_spec v16.0).
 * 
 * <p>Two types of nodes:
 * 1. Boolean operator node: { "op": "and" | "or", "conditions": [...] }
 * 2. Comparison predicate node: { "op": "=" | "!=" | ">" | ">=" | "<" | "<=" | "contains" | "starts_with" | "in" | "not_in", "column": "...", "value": { "label": "v1" } or { "labels": ["v1", "v2"] } }
 */
@JsonDeserialize(using = ExpressionTree.Deserializer.class)
public sealed interface ExpressionTree {
  
  /**
   * Custom deserializer for ExpressionTree sealed interface.
   */
  class Deserializer extends JsonDeserializer<ExpressionTree> {
    @Override
    public ExpressionTree deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      
      if (node.isNull()) {
        return null;
      }
      
      if (!node.has("op")) {
        throw new IOException("ExpressionTree node must have 'op' field");
      }
      
      String op = node.get("op").asText();
      
      // Check if it's a boolean operator (has "conditions" field)
      if (node.has("conditions")) {
        List<ExpressionTree> conditions = new ArrayList<>();
        JsonNode conditionsNode = node.get("conditions");
        if (conditionsNode.isArray()) {
          for (JsonNode condition : conditionsNode) {
            conditions.add(p.getCodec().treeToValue(condition, ExpressionTree.class));
          }
        }
        return new BooleanNode(op, conditions);
      } else if (node.has("column") && node.has("value")) {
        // It's a comparison node (has "column" and "value" fields)
        String column = node.get("column").asText();
        JsonNode valueNode = node.get("value");
        Placeholder value;
        if (valueNode.has("label")) {
          // Single label
          value = new Placeholder.SingleLabel(valueNode.get("label").asText());
        } else if (valueNode.has("labels")) {
          // Multi-label
          List<String> labels = new ArrayList<>();
          JsonNode labelsNode = valueNode.get("labels");
          if (labelsNode.isArray()) {
            for (JsonNode label : labelsNode) {
              labels.add(label.asText());
            }
          }
          value = new Placeholder.MultiLabel(labels);
        } else {
          throw new IOException("Placeholder must have either 'label' or 'labels' field");
        }
        return new ComparisonNode(op, column, value);
      } else {
        throw new IOException("ExpressionTree node must have either 'conditions' (BooleanNode) or 'column' and 'value' (ComparisonNode)");
      }
    }
  }
  
  /**
   * Boolean operator node (AND/OR).
   */
  record BooleanNode(
      @JsonProperty("op") String op, // "and" | "or"
      @JsonProperty("conditions") List<ExpressionTree> conditions
  ) implements ExpressionTree {}
  
  /**
   * Comparison predicate node.
   */
  record ComparisonNode(
      @JsonProperty("op") String op, // "=" | "!=" | ">" | ">=" | "<" | "<=" | "contains" | "starts_with" | "in" | "not_in"
      @JsonProperty("column") String column,
      @JsonProperty("value") Placeholder value // { "label": "v1" } or { "labels": ["v1", "v2"] }
  ) implements ExpressionTree {}
}

