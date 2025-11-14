package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

public class EndpointInvoker {

  private final String baseUrl;
  private final String pathTemplate;
  private final String method;
  private final Operation operation;

  public EndpointInvoker(String baseUrl, String pathTemplate, String method, Operation operation) {
    this.baseUrl = baseUrl;
    this.pathTemplate = pathTemplate;
    this.method = method;
    this.operation = operation;
  }

  public JsonNode invoke(JsonNode input) throws Exception {
    // 1️⃣ Build final path with {pathParam} replacements
    String finalPath = buildPath(pathTemplate, input);

    // 2️⃣ Build query string (?a=b&c=d)
    String query = buildQuery(operation.getParameters(), input);
    String fullUrl = baseUrl + finalPath + (query.isEmpty() ? "" : "?" + query);

    // 3️⃣ Prepare HTTP request builder
    HttpRequest.Builder builder =
        HttpRequest.newBuilder().uri(URI.create(fullUrl)).header("Accept", "application/json");

    // 4️⃣ Add headers and cookies
    addHeaders(operation.getParameters(), input, builder);
    addCookies(operation.getParameters(), input, builder);

    // 5️⃣ Build request body (if needed)
    String body = buildBody(operation.getRequestBody(), input);

    if (hasBody()) {
      builder
          .method(method, HttpRequest.BodyPublishers.ofString(body))
          .header("Content-Type", "application/json");
    } else {
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    }

    // 6️⃣ Send request
    HttpClient client = HttpClient.newHttpClient();
    HttpResponse<String> response =
        client.send(builder.build(), HttpResponse.BodyHandlers.ofString());

    return HttpUtils.toJsonNode(response.body());
  }

  private boolean hasBody() {
    return method.equalsIgnoreCase("POST")
        || method.equalsIgnoreCase("PUT")
        || method.equalsIgnoreCase("PATCH");
  }

  /** Replace {path} variables */
  private String buildPath(String path, JsonNode input) {
    Matcher m = Pattern.compile("\\{(\\w+)}").matcher(path);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String param = m.group(1);
      String value = input.has(param) ? input.get(param).asText() : "";
      m.appendReplacement(sb, value);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  /** Build query string (?param=value) */
  private String buildQuery(List<Parameter> parameters, JsonNode input) {
    if (parameters == null) return "";
    List<String> parts = new ArrayList<>();

    for (Parameter p : parameters) {
      if (!"query".equalsIgnoreCase(p.getIn())) continue;
      String name = p.getName();
      if (input.has(name)) {
        String value = input.get(name).asText();
        parts.add(
            URLEncoder.encode(name, StandardCharsets.UTF_8)
                + "="
                + URLEncoder.encode(value, StandardCharsets.UTF_8));
      }
    }
    return String.join("&", parts);
  }

  /** Add header parameters */
  private void addHeaders(List<Parameter> parameters, JsonNode input, HttpRequest.Builder builder) {
    if (parameters == null) return;
    for (Parameter p : parameters) {
      if (!"header".equalsIgnoreCase(p.getIn())) continue;
      String name = p.getName();
      if (input.has(name)) {
        builder.header(name, input.get(name).asText());
      }
    }
  }

  /** Add cookie parameters */
  private void addCookies(List<Parameter> parameters, JsonNode input, HttpRequest.Builder builder) {
    if (parameters == null) return;
    List<String> cookies = new ArrayList<>();

    for (Parameter p : parameters) {
      if (!"cookie".equalsIgnoreCase(p.getIn())) continue;
      String name = p.getName();
      if (input.has(name)) {
        cookies.add(name + "=" + input.get(name).asText());
      }
    }

    if (!cookies.isEmpty()) {
      builder.header("Cookie", String.join("; ", cookies));
    }
  }

  /** Build request body from "body" field */
  private String buildBody(RequestBody requestBody, JsonNode input) {
    if (requestBody == null || !hasBody()) return "";
    if (input.has("body")) {
      return input.get("body").toString();
    }

    Map<String, MediaType> content = requestBody.getContent();
    if (content != null && content.containsKey("application/json")) {
      return input.toString(); // fallback
    }
    return "";
  }
}
