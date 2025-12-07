package com.gentoro.onemcp.cache.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a complete DAG execution plan.
 * 
 * <p>Structure:
 * - nodes: array of DagNode objects
 * - entryPoint: ID of the entry node (optional, defaults to first node)
 */
public class DagPlan {
  private final List<DagNode> nodes;
  private final String entryPoint;

  public DagPlan(List<DagNode> nodes, String entryPoint) {
    this.nodes = nodes != null ? nodes : new ArrayList<>();
    this.entryPoint = entryPoint;
  }

  public static DagPlan fromJson(JsonNode planJson) {
    List<DagNode> nodes = new ArrayList<>();
    
    if (planJson.has("nodes") && planJson.get("nodes").isArray()) {
      ArrayNode nodesArray = (ArrayNode) planJson.get("nodes");
      for (JsonNode nodeJson : nodesArray) {
        nodes.add(DagNode.fromJson(nodeJson));
      }
    }

    String entryPoint = planJson.has("entryPoint") 
        ? planJson.get("entryPoint").asText() 
        : (nodes.isEmpty() ? null : nodes.get(0).getId());

    return new DagPlan(nodes, entryPoint);
  }

  public static DagPlan fromJsonString(String json) {
    try {
      JsonNode planJson = JacksonUtility.getJsonMapper().readTree(json);
      return fromJson(planJson);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid DAG plan JSON: " + e.getMessage(), e);
    }
  }

  public List<DagNode> getNodes() {
    return nodes;
  }

  public String getEntryPoint() {
    return entryPoint;
  }

  public Map<String, DagNode> getNodeMap() {
    Map<String, DagNode> map = new HashMap<>();
    for (DagNode node : nodes) {
      map.put(node.getId(), node);
    }
    return map;
  }

  public String toJson() {
    try {
      ObjectNode plan = JacksonUtility.getJsonMapper().createObjectNode();
      ArrayNode nodesArray = JacksonUtility.getJsonMapper().createArrayNode();
      for (DagNode node : nodes) {
        nodesArray.add(node.toJson());
      }
      plan.set("nodes", nodesArray);
      if (entryPoint != null) {
        plan.put("entryPoint", entryPoint);
      }
      return JacksonUtility.getJsonMapper().writeValueAsString(plan);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize DAG plan", e);
    }
  }
}

