package com.gentoro.onemcp.indexing.graph.nodes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Graph node representing documentation.
 *
 * <p>Documentation nodes are extracted sections of documentation that provide detailed information
 * about entities, operations, or fields. They are linked to their corresponding graph elements to
 * enable rich contextual retrieval.
 */
public class DocumentationNode implements GraphNode {
  private final String key;
  private final String title;
  private final String content;
  private final String
      docType; // entity_description, operation_guide, field_reference, concept, example_explanation
  private final String sourceFile;
  private final List<String>
      relatedKeys; // Keys of entities/operations/fields this documentation relates to
  private final String serviceSlug;
  private final Map<String, String> metadata; // Additional context (section, category, etc.)

  public DocumentationNode(
      String key,
      String title,
      String content,
      String docType,
      String sourceFile,
      List<String> relatedKeys,
      String serviceSlug,
      Map<String, String> metadata) {
    this.key = key;
    this.title = title;
    this.content = content;
    this.docType = docType;
    this.sourceFile = sourceFile;
    this.relatedKeys = relatedKeys;
    this.serviceSlug = serviceSlug;
    this.metadata = metadata != null ? metadata : new HashMap<>();
  }

  public DocumentationNode(
      String key,
      String title,
      String content,
      String docType,
      String sourceFile,
      List<String> relatedKeys,
      String serviceSlug) {
    this(key, title, content, docType, sourceFile, relatedKeys, serviceSlug, new HashMap<>());
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getNodeType() {
    return "documentation";
  }

  public String getTitle() {
    return title;
  }

  public String getContent() {
    return content;
  }

  public String getDocType() {
    return docType;
  }

  public String getSourceFile() {
    return sourceFile;
  }

  public List<String> getRelatedKeys() {
    return relatedKeys;
  }

  public String getServiceSlug() {
    return serviceSlug;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("_key", key);
    map.put("nodeType", getNodeType());
    map.put("title", title);
    map.put("content", content);
    map.put("docType", docType);
    map.put("sourceFile", sourceFile);
    map.put("relatedKeys", relatedKeys);
    map.put("serviceSlug", serviceSlug);
    if (metadata != null && !metadata.isEmpty()) {
      map.put("metadata", metadata);
    }
    return map;
  }
}
