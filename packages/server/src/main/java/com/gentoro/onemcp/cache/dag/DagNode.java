package com.gentoro.onemcp.cache.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for DAG nodes in the execution plan system.
 * 
 * <p>All nodes share a common structure:
 * - id: unique node identifier
 * - type: node type (ConvertValue, ApiCall, Filter, etc.)
 * - config: node-specific configuration
 * - inputs: optional map of input names to source node IDs
 */
public class DagNode {
  private final String id;
  private final String type;
  private final ObjectNode config;
  private final Map<String, String> inputs;

  public DagNode(String id, String type, ObjectNode config, Map<String, String> inputs) {
    this.id = id;
    this.type = type;
    this.config = config != null ? config : com.gentoro.onemcp.utility.JacksonUtility.getJsonMapper().createObjectNode();
    this.inputs = inputs != null ? inputs : new HashMap<>();
  }

  public static DagNode fromJson(JsonNode nodeJson) {
    String id = nodeJson.has("id") ? nodeJson.get("id").asText() : null;
    String type = nodeJson.has("type") ? nodeJson.get("type").asText() : null;
    ObjectNode config = nodeJson.has("config") && nodeJson.get("config").isObject() 
        ? (ObjectNode) nodeJson.get("config") 
        : com.gentoro.onemcp.utility.JacksonUtility.getJsonMapper().createObjectNode();
    
    Map<String, String> inputs = new HashMap<>();
    if (nodeJson.has("inputs") && nodeJson.get("inputs").isObject()) {
      JsonNode inputsNode = nodeJson.get("inputs");
      inputsNode.fields().forEachRemaining(entry -> {
        if (entry.getValue().isTextual()) {
          inputs.put(entry.getKey(), entry.getValue().asText());
        }
      });
    }

    if (id == null || type == null) {
      throw new IllegalArgumentException("DAG node must have 'id' and 'type' fields");
    }

    return new DagNode(id, type, config, inputs);
  }

  public String getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public ObjectNode getConfig() {
    return config;
  }

  public Map<String, String> getInputs() {
    return inputs;
  }

  public ObjectNode toJson() {
    ObjectNode node = com.gentoro.onemcp.utility.JacksonUtility.getJsonMapper().createObjectNode();
    node.put("id", id);
    node.put("type", type);
    node.set("config", config);
    if (!inputs.isEmpty()) {
      ObjectNode inputsNode = com.gentoro.onemcp.utility.JacksonUtility.getJsonMapper().createObjectNode();
      inputs.forEach(inputsNode::put);
      node.set("inputs", inputsNode);
    }
    return node;
  }
}

