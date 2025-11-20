package com.gentoro.onemcp.http;

import com.gentoro.onemcp.logging.InferenceLogger;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import okhttp3.*;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;

public class LoggingInterceptor implements Interceptor {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(LoggingInterceptor.class);
  
  private final InferenceLogger inferenceLogger;

  public LoggingInterceptor(InferenceLogger inferenceLogger) {
    if (inferenceLogger == null) {
      throw new IllegalArgumentException("InferenceLogger cannot be null");
    }
    this.inferenceLogger = inferenceLogger;
  }

  @NotNull
  @Override
  public Response intercept(Chain chain) throws IOException {
    Request request = chain.request();

    long startTime = System.nanoTime();
    String requestBody = bodyToString(request);
    log.debug(
        "➡️ Sending request {}\nHeaders:\n{}\nBody:\n{}\n",
        request.url(),
        request.headers(),
        requestBody);

    Response response = null;
    String errorMessage = null;
    String errorType = null;
    
    try {
      response = chain.proceed(request);
    } catch (java.net.SocketTimeoutException e) {
      errorType = "Timeout";
      errorMessage = e.getMessage();
      if (errorMessage == null || errorMessage.isEmpty()) {
        errorMessage = "Request timed out";
      }
      long endTime = System.nanoTime();
      long durationMs = (endTime - startTime) / 1_000_000;
      
      // Log error to production logger
      Map<String, String> requestHeadersMap = new HashMap<>();
      request.headers().forEach(pair -> requestHeadersMap.put(pair.getFirst(), pair.getSecond()));
      
        inferenceLogger.logApiCallError(
          request.method(),
          request.url().toString(),
          requestHeadersMap,
          requestBody,
          0, // No status code for timeout
          errorType,
          errorMessage,
          durationMs);
      
      log.warn("Request timed out: {} {} ({}ms)", request.method(), request.url(), durationMs);
      throw e;
    } catch (java.net.ConnectException e) {
      errorType = "Connection Error";
      errorMessage = e.getMessage();
      if (errorMessage == null || errorMessage.isEmpty()) {
        errorMessage = "Could not connect to server";
      }
      long endTime = System.nanoTime();
      long durationMs = (endTime - startTime) / 1_000_000;
      
      // Log error to production logger
      Map<String, String> requestHeadersMap = new HashMap<>();
      request.headers().forEach(pair -> requestHeadersMap.put(pair.getFirst(), pair.getSecond()));
      
        inferenceLogger.logApiCallError(
          request.method(),
          request.url().toString(),
          requestHeadersMap,
          requestBody,
          0, // No status code for connection error
          errorType,
          errorMessage,
          durationMs);
      
      log.warn("Connection error: {} {} ({}ms): {}", request.method(), request.url(), durationMs, errorMessage);
      throw e;
    } catch (IOException e) {
      errorType = "IO Error";
      errorMessage = e.getMessage();
      if (errorMessage == null || errorMessage.isEmpty()) {
        errorMessage = e.getClass().getSimpleName();
      }
      long endTime = System.nanoTime();
      long durationMs = (endTime - startTime) / 1_000_000;
      
      // Log error to production logger
      Map<String, String> requestHeadersMap = new HashMap<>();
      request.headers().forEach(pair -> requestHeadersMap.put(pair.getFirst(), pair.getSecond()));
      
        inferenceLogger.logApiCallError(
          request.method(),
          request.url().toString(),
          requestHeadersMap,
          requestBody,
          0, // No status code for IO error
          errorType,
          errorMessage,
          durationMs);
      
      log.warn("IO error: {} {} ({}ms): {}", request.method(), request.url(), durationMs, errorMessage);
      throw e;
    }

    long endTime = System.nanoTime();
    long durationMs = (endTime - startTime) / 1_000_000;
    
    // Read response body safely (clone it)
    String responseBodyString = "";
    try {
      ResponseBody peekedBody = response.peekBody(Long.MAX_VALUE);
      if (peekedBody != null) {
        responseBodyString = peekedBody.string();
        // Handle null or empty response
        if (responseBodyString == null) {
          responseBodyString = "";
        }
      }
    } catch (Exception e) {
      log.debug("Could not read response body", e);
      responseBodyString = "";
    }
    
    log.debug(
        "⬅️ Received response for {} in {} ms\nStatus: {}\nHeaders:\n{}\n",
        response.request().url(),
        String.format("%.1f", durationMs / 1e6d),
        response.code(),
        response.headers());
    if (!responseBodyString.isEmpty()) {
      log.trace("Response body:\n{}\n", responseBodyString);
    } else {
      log.trace("Response body: [empty]\n");
    }

    // Log to production logger
    Map<String, String> requestHeadersMap = new HashMap<>();
    request.headers().forEach(pair -> requestHeadersMap.put(pair.getFirst(), pair.getSecond()));
    
    Map<String, String> responseHeadersMap = new HashMap<>();
    response.headers().forEach(pair -> responseHeadersMap.put(pair.getFirst(), pair.getSecond()));
    
    inferenceLogger.logApiCall(
        request.method(),
        request.url().toString(),
        requestHeadersMap,
        requestBody,
        response.code(),
        responseHeadersMap,
        responseBodyString,
        durationMs);

    return response;
  }

  private static String bodyToString(Request request) {
    try {
      Request copy = request.newBuilder().build();
      Buffer buffer = new Buffer();
      if (copy.body() != null) copy.body().writeTo(buffer);
      return buffer.readUtf8();
    } catch (IOException e) {
      return "(error reading body)";
    }
  }
}
