package com.gentoro.onemcp.indexing.graph.nodes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Graph node representing an entity (OpenAPI tag).
 *
 * <p>Entities are extracted from OpenAPI specification tags and represent high-level business
 * concepts or service categories (e.g., "Analytics", "Metadata", "System").
 */
public class EntityNode implements GraphNode {
  private final String key;
  private final String name;
  private final String description;
  private final String serviceSlug;
  private final List<String> associatedOperations;
  private final String source;
  private final List<String> attributes;
  private final String domain;

  public EntityNode(
      String key,
      String name,
      String description,
      String serviceSlug,
      List<String> associatedOperations) {
    this(key, name, description, serviceSlug, associatedOperations, null, null, null);
  }

  public EntityNode(
      String key,
      String name,
      String description,
      String serviceSlug,
      List<String> associatedOperations,
      String source,
      List<String> attributes,
      String domain) {
    this.key = key;
    this.name = name;
    this.description = description;
    this.serviceSlug = serviceSlug;
    this.associatedOperations = associatedOperations;
    this.source = source;
    this.attributes = attributes;
    this.domain = domain;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getNodeType() {
    return "entity";
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getServiceSlug() {
    return serviceSlug;
  }

  public List<String> getAssociatedOperations() {
    return associatedOperations;
  }

  public String getSource() {
    return source;
  }

  public List<String> getAttributes() {
    return attributes;
  }

  public String getDomain() {
    return domain;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("_key", key);
    map.put("nodeType", getNodeType());
    map.put("name", name); // Used for node label in ArangoDB UI
    map.put("label", name); // Alternative label field
    map.put("description", description);
    map.put("serviceSlug", serviceSlug);
    map.put("associatedOperations", associatedOperations);
    if (source != null) map.put("source", source);
    if (attributes != null) map.put("attributes", attributes);
    if (domain != null) map.put("domain", domain);
    return map;
  }
}
