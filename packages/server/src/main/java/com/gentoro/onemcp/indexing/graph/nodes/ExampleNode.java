package com.gentoro.onemcp.indexing.graph.nodes;

import java.util.HashMap;
import java.util.Map;

/**
 * Graph node representing an API example (request/response pair).
 *
 * <p>Examples are extracted from OpenAPI specifications and provide concrete usage patterns for
 * operations. They include sample requests and expected responses.
 */
public class ExampleNode implements GraphNode {
  private final String key;
  private final String name;
  private final String summary;
  private final String description;
  private final String requestBody;
  private final String responseBody;
  private final String responseStatus;
  private final String operationKey;
  private final String serviceSlug;

  public ExampleNode(
      String key,
      String name,
      String summary,
      String description,
      String requestBody,
      String responseBody,
      String responseStatus,
      String operationKey,
      String serviceSlug) {
    this.key = key;
    this.name = name;
    this.summary = summary;
    this.description = description;
    this.requestBody = requestBody;
    this.responseBody = responseBody;
    this.responseStatus = responseStatus;
    this.operationKey = operationKey;
    this.serviceSlug = serviceSlug;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getNodeType() {
    return "example";
  }

  public String getName() {
    return name;
  }

  public String getSummary() {
    return summary;
  }

  public String getDescription() {
    return description;
  }

  public String getRequestBody() {
    return requestBody;
  }

  public String getResponseBody() {
    return responseBody;
  }

  public String getResponseStatus() {
    return responseStatus;
  }

  public String getOperationKey() {
    return operationKey;
  }

  public String getServiceSlug() {
    return serviceSlug;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("_key", key);
    map.put("nodeType", getNodeType());
    map.put("name", name); // Used for node label in ArangoDB UI
    map.put("label", name); // Alternative label field
    map.put("summary", summary);
    map.put("description", description);
    map.put("requestBody", requestBody);
    map.put("responseBody", responseBody);
    map.put("responseStatus", responseStatus);
    map.put("operationKey", operationKey);
    map.put("serviceSlug", serviceSlug);
    return map;
  }
}
