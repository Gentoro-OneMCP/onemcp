package com.gentoro.onemcp.mcp.model;

import java.util.List;

public class ContextItem {
  private ContextKind kind;
  private List<Header> headers;
  private List<Cookie> cookies;

  public ContextKind getKind() {
    return kind;
  }

  public void setKind(ContextKind kind) {
    this.kind = kind;
  }

  public List<Header> getHeaders() {
    return headers;
  }

  public void setHeaders(List<Header> headers) {
    this.headers = headers;
  }

  public List<Cookie> getCookies() {
    return cookies;
  }

  public void setCookies(List<Cookie> cookies) {
    this.cookies = cookies;
  }
}
