package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Endpoint index for a table (cache_spec v6.0).
 *
 * <p>Stored in cache/index/<table>.json, contains all API endpoints
 * that operate on a specific table.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableEndpointIndex {
  @JsonProperty("table")
  private String table;

  @JsonProperty("endpoints")
  private List<EndpointInfo> endpoints = new ArrayList<>();

  public TableEndpointIndex() {}

  public TableEndpointIndex(String table, List<EndpointInfo> endpoints) {
    this.table = table;
    this.endpoints = endpoints != null ? endpoints : new ArrayList<>();
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<EndpointInfo> getEndpoints() {
    return endpoints;
  }

  public void setEndpoints(List<EndpointInfo> endpoints) {
    this.endpoints = endpoints != null ? endpoints : new ArrayList<>();
  }

  /**
   * Information about an API endpoint.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EndpointInfo {
    @JsonProperty("method")
    private String method; // GET, POST, PUT, PATCH, DELETE

    @JsonProperty("path")
    private String path;

    @JsonProperty("operationId")
    private String operationId;

    @JsonProperty("params")
    private Map<String, Object> params; // Path/query parameters

    @JsonProperty("request_schema")
    private Map<String, Object> requestSchema; // Request body schema

    @JsonProperty("response_schema")
    private Map<String, Object> responseSchema; // Response schema

    @JsonProperty("summary")
    private String summary;

    @JsonProperty("description")
    private String description;

    public EndpointInfo() {}

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public String getOperationId() {
      return operationId;
    }

    public void setOperationId(String operationId) {
      this.operationId = operationId;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    public void setParams(Map<String, Object> params) {
      this.params = params;
    }

    public Map<String, Object> getRequestSchema() {
      return requestSchema;
    }

    public void setRequestSchema(Map<String, Object> requestSchema) {
      this.requestSchema = requestSchema;
    }

    public Map<String, Object> getResponseSchema() {
      return responseSchema;
    }

    public void setResponseSchema(Map<String, Object> responseSchema) {
      this.responseSchema = responseSchema;
    }

    public String getSummary() {
      return summary;
    }

    public void setSummary(String summary) {
      this.summary = summary;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }
}

