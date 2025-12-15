package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import java.util.*;

/**
 * Canonical node record used by the unified v2 GraphDriver.
 *
 * <p>Fields are intentionally backend-agnostic and align with the required output categories.
 *
 * <p>Required fields by category: - API_DOCUMENTATION: content(markdown), apiSlug, entities,
 * operations(optional) - API_OPERATION_DOCUMENTATION: content(markdown), apiSlug, operationId,
 * entities, operations - API_OPERATION_INPUT: content(json), apiSlug, operationId, entities,
 * operations - API_OPERATION_OUTPUT: content(json), apiSlug, operationId, entities, operations -
 * API_OPERATION_EXAMPLE: content(markdown), apiSlug, operationId, entities, operations -
 * DOCS_CHUNK: content(markdown), docPath, entities, operations(optional), parentDocumentKey(optional)
 * - DOCUMENT: content(optional), docPath, entities(optional)
 */
public class GraphNodeRecord {
  private String key; // unique
  private KnowledgeNodeType nodeType;
  private String apiSlug; // optional depending on type
  private String operationId; // for operation-scope nodes
  private String content; // markdown or json depending on type
  private String contentFormat; // "markdown" | "json"
  private String docPath; // only for DOCS_CHUNK and DOCUMENT
  private String title; // optional (useful for examples)
  private String summary; // optional (useful for examples)
  private String parentDocumentKey; // for DOCS_CHUNK: key of parent DOCUMENT node
  private List<String> entities = new ArrayList<>();
  private List<String> operations = new ArrayList<>();

  public GraphNodeRecord() {}

  public GraphNodeRecord(String key, KnowledgeNodeType nodeType) {
    this.key = Objects.requireNonNull(key, "key");
    this.nodeType = Objects.requireNonNull(nodeType, "nodeType");
  }

  public String getKey() {
    return key;
  }

  public GraphNodeRecord setKey(String key) {
    this.key = key;
    return this;
  }

  public KnowledgeNodeType getNodeType() {
    return nodeType;
  }

  public GraphNodeRecord setNodeType(KnowledgeNodeType nodeType) {
    this.nodeType = nodeType;
    return this;
  }

  public String getApiSlug() {
    return apiSlug;
  }

  public GraphNodeRecord setApiSlug(String apiSlug) {
    this.apiSlug = apiSlug;
    return this;
  }

  public String getOperationId() {
    return operationId;
  }

  public GraphNodeRecord setOperationId(String operationId) {
    this.operationId = operationId;
    return this;
  }

  public String getContent() {
    return content;
  }

  public GraphNodeRecord setContent(String content) {
    this.content = content;
    return this;
  }

  public String getContentFormat() {
    return contentFormat;
  }

  public GraphNodeRecord setContentFormat(String contentFormat) {
    this.contentFormat = contentFormat;
    return this;
  }

  public String getDocPath() {
    return docPath;
  }

  public GraphNodeRecord setDocPath(String docPath) {
    this.docPath = docPath;
    return this;
  }

  public String getTitle() {
    return title;
  }

  public GraphNodeRecord setTitle(String title) {
    this.title = title;
    return this;
  }

  public String getSummary() {
    return summary;
  }

  public GraphNodeRecord setSummary(String summary) {
    this.summary = summary;
    return this;
  }

  public List<String> getEntities() {
    return entities;
  }

  public GraphNodeRecord setEntities(List<String> entities) {
    this.entities = new ArrayList<>(Objects.requireNonNullElse(entities, Collections.emptyList()));
    return this;
  }

  public List<String> getOperations() {
    return operations;
  }

  public GraphNodeRecord setOperations(List<String> operations) {
    this.operations =
        new ArrayList<>(Objects.requireNonNullElse(operations, Collections.emptyList()));
    return this;
  }

  public String getParentDocumentKey() {
    return parentDocumentKey;
  }

  public GraphNodeRecord setParentDocumentKey(String parentDocumentKey) {
    this.parentDocumentKey = parentDocumentKey;
    return this;
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("key", key);
    map.put("nodeType", nodeType.name());
    if (apiSlug != null) map.put("apiSlug", apiSlug);
    if (operationId != null) map.put("operationId", operationId);
    if (content != null) map.put("content", content);
    if (contentFormat != null) map.put("contentFormat", contentFormat);
    if (docPath != null) map.put("docPath", docPath);
    if (title != null) map.put("title", title);
    if (summary != null) map.put("summary", summary);
    if (parentDocumentKey != null) map.put("parentDocumentKey", parentDocumentKey);
    map.put("entities", new ArrayList<>(entities));
    map.put("operations", new ArrayList<>(operations));
    return map;
  }

  public static GraphNodeRecord fromMap(Map<String, Object> map) {
    GraphNodeRecord r = new GraphNodeRecord();
    r.key = Objects.toString(map.get("key"), null);
    Object nt = map.get("nodeType");
    if (nt != null) r.nodeType = KnowledgeNodeType.valueOf(nt.toString());
    r.apiSlug = (String) map.get("apiSlug");
    r.operationId = (String) map.get("operationId");
    r.content = (String) map.get("content");
    r.contentFormat = (String) map.get("contentFormat");
    r.docPath = (String) map.get("docPath");
    r.title = (String) map.get("title");
    r.summary = (String) map.get("summary");
    r.parentDocumentKey = (String) map.get("parentDocumentKey");
    Object ent = map.get("entities");
    if (ent instanceof Collection<?> c) {
      for (Object e : c) r.entities.add(Objects.toString(e));
    }
    Object ops = map.get("operations");
    if (ops instanceof Collection<?> c) {
      for (Object o : c) r.operations.add(Objects.toString(o));
    }
    return r;
  }
}
