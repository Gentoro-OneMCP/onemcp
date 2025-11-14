package com.gentoro.onemcp.context;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.utility.StringUtility;

public class Operation {
  private volatile OneMcp oneMcp;
  private volatile Service service;
  private String documentationUri;
  private String operation;
  private String method;
  private String path;
  private String summary;

  public Operation() {}

  public Operation(
      String documentationUri, String operation, String method, String path, String summary) {
    this.documentationUri = documentationUri;
    this.operation = operation;
    this.method = method;
    this.path = path;
    this.summary = summary;
  }

  public void setOneMcp(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  public void setService(Service service) {
    this.service = service;
  }

  public String getDocumentation(Long ident) {
    return StringUtility.formatWithIndent(
        oneMcp
            .knowledgeBase()
            .getDocument(
                "kb:///services/%s/operations/%s.md".formatted(service.getSlug(), getOperation()))
            .content(),
        ident.intValue());
  }

  public String getSummary(Long ident) {
    return StringUtility.formatWithIndent(getSummary(), ident.intValue());
  }

  public String getDocumentationUri() {
    return documentationUri;
  }

  public void setDocumentationUri(String documentationUri) {
    this.documentationUri = documentationUri;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

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

  public String getSummary() {
    return summary;
  }

  public void setSummary(String summary) {
    this.summary = summary;
  }
}
