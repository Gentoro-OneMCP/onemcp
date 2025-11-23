package com.gentoro.onemcp.indexing.graph.nodes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Graph node representing an API operation (endpoint).
 *
 * <p>Operations are extracted from OpenAPI specifications and include the method, path, signature,
 * examples, and documentation.
 */
public class OperationNode implements GraphNode {
  private final String key;
  private final String operationId;
  private final String method;
  private final String path;
  private final String summary;
  private final String description;
  private final String serviceSlug;
  private final List<String> tags;
  private final String signature;
  private final List<String> exampleKeys;
  private final String documentationUri;
  private final String requestSchema;
  private final String responseSchema;
  private final List<String> examples;
  private final String category;
  private final String primaryEntity;

  public OperationNode(
      String key,
      String operationId,
      String method,
      String path,
      String summary,
      String description,
      String serviceSlug,
      List<String> tags,
      String signature,
      List<String> exampleKeys,
      String documentationUri) {
    this(
        key,
        operationId,
        method,
        path,
        summary,
        description,
        serviceSlug,
        tags,
        signature,
        exampleKeys,
        documentationUri,
        null,
        null,
        null,
        null,
        null);
  }

  public OperationNode(
      String key,
      String operationId,
      String method,
      String path,
      String summary,
      String description,
      String serviceSlug,
      List<String> tags,
      String signature,
      List<String> exampleKeys,
      String documentationUri,
      String requestSchema,
      String responseSchema,
      List<String> examples) {
    this(
        key,
        operationId,
        method,
        path,
        summary,
        description,
        serviceSlug,
        tags,
        signature,
        exampleKeys,
        documentationUri,
        requestSchema,
        responseSchema,
        examples,
        null,
        null);
  }

  public OperationNode(
      String key,
      String operationId,
      String method,
      String path,
      String summary,
      String description,
      String serviceSlug,
      List<String> tags,
      String signature,
      List<String> exampleKeys,
      String documentationUri,
      String requestSchema,
      String responseSchema,
      List<String> examples,
      String category,
      String primaryEntity) {
    this.key = key;
    this.operationId = operationId;
    this.method = method;
    this.path = path;
    this.summary = summary;
    this.description = description;
    this.serviceSlug = serviceSlug;
    this.tags = tags;
    this.signature = signature;
    this.exampleKeys = exampleKeys;
    this.documentationUri = documentationUri;
    this.requestSchema = requestSchema;
    this.responseSchema = responseSchema;
    this.examples = examples;
    this.category = category;
    this.primaryEntity = primaryEntity;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getNodeType() {
    return "operation";
  }

  public String getOperationId() {
    return operationId;
  }

  public String getMethod() {
    return method;
  }

  public String getPath() {
    return path;
  }

  public String getSummary() {
    return summary;
  }

  public String getDescription() {
    return description;
  }

  public String getServiceSlug() {
    return serviceSlug;
  }

  public List<String> getTags() {
    return tags;
  }

  public String getSignature() {
    return signature;
  }

  public List<String> getExampleKeys() {
    return exampleKeys;
  }

  public String getDocumentationUri() {
    return documentationUri;
  }

  public String getRequestSchema() {
    return requestSchema;
  }

  public String getResponseSchema() {
    return responseSchema;
  }

  public List<String> getExamples() {
    return examples;
  }

  public String getCategory() {
    return category;
  }

  public String getPrimaryEntity() {
    return primaryEntity;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("_key", key);
    map.put("nodeType", getNodeType());
    map.put("operationId", operationId);
    // Create display name for node label
    String displayName = method != null ? method.toUpperCase() + " " + path : operationId;
    map.put("name", displayName); // Used for node label in ArangoDB UI
    map.put("label", displayName); // Alternative label field
    map.put("method", method);
    map.put("path", path);
    map.put("summary", summary);
    map.put("description", description);
    map.put("serviceSlug", serviceSlug);
    map.put("tags", tags);
    map.put("signature", signature);
    map.put("exampleKeys", exampleKeys);
    map.put("documentationUri", documentationUri);
    if (requestSchema != null) map.put("requestSchema", requestSchema);
    if (responseSchema != null) map.put("responseSchema", responseSchema);
    if (examples != null) map.put("examples", examples);
    if (category != null) map.put("category", category);
    if (primaryEntity != null) map.put("primaryEntity", primaryEntity);
    return map;
  }
}
