package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EndpointInvoker {

  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(EndpointInvoker.class);

  private final String baseUrl;
  private final String pathTemplate;
  private final String method;
  private final Operation operation;
  private com.gentoro.onemcp.logging.InferenceLogger inferenceLogger;

  public EndpointInvoker(String baseUrl, String pathTemplate, String method, Operation operation) {
    this.baseUrl = baseUrl;
    this.pathTemplate = pathTemplate;
    this.method = method;
    this.operation = operation;
    this.inferenceLogger = null;
  }

  // Set inference logger for API call logging
  public void setInferenceLogger(com.gentoro.onemcp.logging.InferenceLogger logger) {
    this.inferenceLogger = logger;
  }

  public JsonNode invoke(JsonNode input) throws Exception {
    // Check if this is a structured HTTP request (new format) or flat input (legacy format)
    if (input.has("path_params") || input.has("query") || input.has("body")) {
      // New structured HTTP request format
      return invokeStructured(input);
    } else {
      // Legacy flat input format - extract from input object
      return invokeLegacy(input);
    }
  }

  /**
   * Invoke with structured HTTP request (new format from http_call node).
   * Input has explicit path_params, query, headers, body fields.
   */
  private JsonNode invokeStructured(JsonNode httpRequest) throws Exception {
    // 1️⃣ Build final path with {pathParam} replacements from path_params
    String finalPath = buildPathFromStructured(pathTemplate, httpRequest);

    // 2️⃣ Build query string from query object
    String query = buildQueryFromStructured(httpRequest);
    String fullUrl = baseUrl + finalPath + (query.isEmpty() ? "" : "?" + query);

    log.debug("EndpointInvoker: {} {} (baseUrl={}, pathTemplate={}, finalPath={})",
        method, fullUrl, baseUrl, pathTemplate, finalPath);

    // 3️⃣ Prepare HTTP request builder
    HttpRequest.Builder builder =
        HttpRequest.newBuilder().uri(URI.create(fullUrl)).header("Accept", "application/json");

    // 4️⃣ Add headers from headers object
    addHeadersFromStructured(httpRequest, builder);

    // 5️⃣ Build request body from body object
    String body = buildBodyFromStructured(httpRequest);

    if (hasBody()) {
      builder
          .method(method, HttpRequest.BodyPublishers.ofString(body))
          .header("Content-Type", "application/json");
    } else {
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    }

    return sendRequest(builder, body, fullUrl);
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

  /**
   * Legacy invoke method - extracts path/query/body from flat input object.
   */
  private JsonNode invokeLegacy(JsonNode input) throws Exception {
    // 1️⃣ Build final path with {pathParam} replacements
    String finalPath = buildPath(pathTemplate, input);

    // 2️⃣ Build query string (?a=b&c=d)
    String query = buildQuery(operation.getParameters(), input);
    String fullUrl = baseUrl + finalPath + (query.isEmpty() ? "" : "?" + query);

    log.debug("EndpointInvoker: {} {} (baseUrl={}, pathTemplate={}, finalPath={})",
        method, fullUrl, baseUrl, pathTemplate, finalPath);

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

    return sendRequest(builder, body, fullUrl);
  }

  /**
   * Send HTTP request and return response.
   */
  private JsonNode sendRequest(HttpRequest.Builder builder, String body, String fullUrl) throws Exception {
    // 6️⃣ Send request
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = builder.build();
    long startTime = System.currentTimeMillis();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    long duration = System.currentTimeMillis() - startTime;

    // Log API call if inference logger is available
    if (inferenceLogger != null) {
      String requestBodyStr = body;
      String responseBodyStr = response.body();
      inferenceLogger.logApiCall(
          method,
          fullUrl,
          response.statusCode(),
          duration,
          requestBodyStr != null ? requestBodyStr : "",
          responseBodyStr != null ? responseBodyStr : "");
    }

    // Check for HTTP error status codes (4xx, 5xx)
    int statusCode = response.statusCode();
    if (statusCode >= 400) {
      String errorMessage = "HTTP " + statusCode;
      String responseBody = response.body();
      
      // Log detailed error information for debugging
      log.warn("HTTP request failed: {} {} -> {} (baseUrl={}, pathTemplate={})",
          method, fullUrl, statusCode, baseUrl, pathTemplate);
      if (responseBody != null && !responseBody.trim().isEmpty()) {
        log.debug("Error response body: {}", responseBody);
        try {
          JsonNode errorJson = HttpUtils.toJsonNode(responseBody);
          if (errorJson.has("error") && errorJson.get("error").isTextual()) {
            errorMessage = "HTTP " + statusCode + ": " + errorJson.get("error").asText();
          } else if (errorJson.has("message") && errorJson.get("message").isTextual()) {
            errorMessage = "HTTP " + statusCode + ": " + errorJson.get("message").asText();
          } else {
            errorMessage = "HTTP " + statusCode + ": " + responseBody;
          }
        } catch (Exception e) {
          // If response body is not JSON, use it as-is
          errorMessage = "HTTP " + statusCode + ": " + responseBody;
        }
      }
      
      // For 405 errors, provide additional diagnostic information
      if (statusCode == 405) {
        log.error("HTTP 405 Method Not Allowed - URL construction details: " +
            "method={}, fullUrl={}, baseUrl={}, pathTemplate={}, " +
            "operationId={}",
            method, fullUrl, baseUrl, pathTemplate,
            operation != null ? operation.getOperationId() : "null");
      }
      
      throw new RuntimeException("API error: " + errorMessage);
    }

    return HttpUtils.toJsonNode(response.body());
  }

  /** Build path from structured HTTP request path_params */
  private String buildPathFromStructured(String path, JsonNode httpRequest) {
    if (!httpRequest.has("path_params")) {
      return path;
    }
    JsonNode pathParams = httpRequest.get("path_params");
    Matcher m = Pattern.compile("\\{(\\w+)}").matcher(path);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String param = m.group(1);
      String value = pathParams.has(param) ? pathParams.get(param).asText() : "";
      m.appendReplacement(sb, value);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  /** Build query string from structured HTTP request query object */
  private String buildQueryFromStructured(JsonNode httpRequest) {
    if (!httpRequest.has("query") || !httpRequest.get("query").isObject()) {
      return "";
    }
    JsonNode queryObj = httpRequest.get("query");
    List<String> parts = new ArrayList<>();
    queryObj.fields().forEachRemaining(entry -> {
      String name = entry.getKey();
      String value = entry.getValue().asText();
      parts.add(
          URLEncoder.encode(name, StandardCharsets.UTF_8)
              + "="
              + URLEncoder.encode(value, StandardCharsets.UTF_8));
    });
    return String.join("&", parts);
  }

  /** Add headers from structured HTTP request headers object */
  private void addHeadersFromStructured(JsonNode httpRequest, HttpRequest.Builder builder) {
    if (!httpRequest.has("headers") || !httpRequest.get("headers").isObject()) {
      return;
    }
    JsonNode headersObj = httpRequest.get("headers");
    headersObj.fields().forEachRemaining(entry -> {
      builder.header(entry.getKey(), entry.getValue().asText());
    });
  }

  /** Build request body from structured HTTP request body object */
  private String buildBodyFromStructured(JsonNode httpRequest) {
    if (!hasBody()) return "";
    if (httpRequest.has("body")) {
      JsonNode bodyNode = httpRequest.get("body");
      if (bodyNode.isObject() || bodyNode.isArray()) {
        return bodyNode.toString();
      } else {
        return bodyNode.asText();
      }
    }
    return "";
  }
}
