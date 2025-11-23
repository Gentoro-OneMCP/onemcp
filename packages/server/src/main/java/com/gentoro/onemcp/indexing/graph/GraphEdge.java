package com.gentoro.onemcp.indexing.graph;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a directed edge (relationship) between two nodes in the knowledge graph.
 *
 * <p>Edges connect nodes and define semantic relationships. Edge types are flexible strings that
 * can represent domain-specific relationships such as "HAS_OPERATION", "CONTAINS", "PRECEDES",
 * "DEPENDS_ON", etc. The system allows any descriptive edge type name to support generic services
 * with domain-specific relationship semantics.
 */
public class GraphEdge {
  private final String fromKey;
  private final String toKey;
  private final String edgeType;
  private final Map<String, Object> properties;
  private final String description;
  private final String strength;

  public GraphEdge(String fromKey, String toKey, String edgeType) {
    this(fromKey, toKey, edgeType, new HashMap<>(), null, null);
  }

  public GraphEdge(String fromKey, String toKey, String edgeType, Map<String, Object> properties) {
    this(fromKey, toKey, edgeType, properties, null, null);
  }

  public GraphEdge(
      String fromKey,
      String toKey,
      String edgeType,
      Map<String, Object> properties,
      String description,
      String strength) {
    if (edgeType == null || edgeType.trim().isEmpty()) {
      throw new IllegalArgumentException("Edge type cannot be null or empty");
    }
    this.fromKey = fromKey;
    this.toKey = toKey;
    this.edgeType = edgeType.trim().toUpperCase();
    this.properties = properties != null ? properties : new HashMap<>();
    this.description = description;
    this.strength = strength;
  }

  public String getFromKey() {
    return fromKey;
  }

  public String getToKey() {
    return toKey;
  }

  public String getEdgeType() {
    return edgeType;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public String getDescription() {
    return description;
  }

  public String getStrength() {
    return strength;
  }

  /**
   * Convert this edge to a map suitable for storing in ArangoDB.
   *
   * @return map of edge properties
   */
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>(properties);
    map.put("edgeType", edgeType);
    if (description != null) map.put("description", description);
    if (strength != null) map.put("strength", strength);
    return map;
  }
}
