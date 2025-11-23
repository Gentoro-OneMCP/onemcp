package com.gentoro.onemcp.indexing.graph.nodes;

import java.util.HashMap;
import java.util.Map;

/**
 * Graph node representing a field/attribute/property of an entity.
 *
 * <p>Fields are extracted from OpenAPI schemas, operation request/response bodies, and examples.
 * They represent individual attributes or properties that belong to entities (e.g., "amount",
 * "date", "category" for a Sales entity).
 */
public class FieldNode implements GraphNode {
  private final String key;
  private final String name;
  private final String description;
  private final String fieldType;
  private final String entityKey;
  private final String serviceSlug;
  private final String source;

  public FieldNode(
      String key,
      String name,
      String description,
      String fieldType,
      String entityKey,
      String serviceSlug,
      String source) {
    this.key = key;
    this.name = name;
    this.description = description;
    this.fieldType = fieldType;
    this.entityKey = entityKey;
    this.serviceSlug = serviceSlug;
    this.source = source;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getNodeType() {
    return "field";
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getFieldType() {
    return fieldType;
  }

  public String getEntityKey() {
    return entityKey;
  }

  public String getServiceSlug() {
    return serviceSlug;
  }

  public String getSource() {
    return source;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("_key", key);
    map.put("nodeType", getNodeType());
    map.put("name", name); // Used for node label in ArangoDB UI
    map.put("label", name); // Alternative label field
    map.put("description", description);
    map.put("fieldType", fieldType);
    map.put("entityKey", entityKey);
    map.put("serviceSlug", serviceSlug);
    if (source != null) map.put("source", source);
    return map;
  }
}
