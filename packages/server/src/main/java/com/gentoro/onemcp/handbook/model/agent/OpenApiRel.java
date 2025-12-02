package com.gentoro.onemcp.handbook.model.agent;

public class OpenApiRel {

  private String path;
  private String httpMethod; // GET | POST | PUT | PATCH | DELETE
  private String operationId;

  public OpenApiRel() {}

  public OpenApiRel(String path, String httpMethod, String operationId) {
    this.path = path;
    this.httpMethod = httpMethod;
    this.operationId = operationId;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  public void setHttpMethod(String httpMethod) {
    this.httpMethod = httpMethod;
  }

  public String getOperationId() {
    return operationId;
  }

  public void setOperationId(String operationId) {
    this.operationId = operationId;
  }
}
