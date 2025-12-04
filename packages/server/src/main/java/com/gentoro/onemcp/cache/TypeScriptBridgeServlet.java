package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.utility.JacksonUtility;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP servlet that bridges TypeScript execution to Java OperationRegistry.
 * 
 * <p>This servlet handles POST requests to /api/execute-operation and invokes
 * operations from the OperationRegistry. The OperationRegistry is stored
 * per-execution in a thread-local or request-scoped map.
 */
public class TypeScriptBridgeServlet extends HttpServlet {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(TypeScriptBridgeServlet.class);
  
  private static final ObjectMapper mapper = JacksonUtility.getJsonMapper();
  
  // Store OperationRegistry per execution UUID (passed as header or query param)
  private static final ConcurrentHashMap<String, OperationRegistry> registryMap = new ConcurrentHashMap<>();
  
  // Thread-local for current execution context
  private static final ThreadLocal<OperationRegistry> currentRegistry = new ThreadLocal<>();
  
  // Also store by execution ID for cross-thread access (HTTP requests may be on different threads)
  static final ConcurrentHashMap<String, OperationRegistry> executionRegistryMap = new ConcurrentHashMap<>();
  private static final ThreadLocal<String> currentExecutionId = new ThreadLocal<>();
  
  /**
   * Set the OperationRegistry for the current execution.
   */
  public static void setCurrentRegistry(OperationRegistry registry) {
    currentRegistry.set(registry);
    String execId = currentExecutionId.get();
    if (execId != null) {
      executionRegistryMap.put(execId, registry);
      log.debug("Stored OperationRegistry in execution map for executionId: {}", execId);
    } else {
      log.warn("setCurrentRegistry called but no executionId is set");
    }
  }
  
  /**
   * Set the execution ID for the current thread.
   */
  public static void setExecutionId(String executionId) {
    currentExecutionId.set(executionId);
  }
  
  /**
   * Get the current execution ID.
   */
  public static String getCurrentExecutionId() {
    return currentExecutionId.get();
  }
  
  /**
   * Clear the OperationRegistry for the current execution.
   */
  public static void clearCurrentRegistry() {
    String execId = currentExecutionId.get();
    if (execId != null) {
      executionRegistryMap.remove(execId);
    }
    currentRegistry.remove();
    currentExecutionId.remove();
  }
  
  /**
   * Clear only the thread-local (keep execution ID in map for async requests).
   */
  public static void clearThreadLocal() {
    currentRegistry.remove();
    currentExecutionId.remove();
  }
  
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");
    
    try {
      // Read request body
      StringBuilder body = new StringBuilder();
      String line;
      while ((line = req.getReader().readLine()) != null) {
        body.append(line);
      }
      
      JsonNode requestJson = mapper.readTree(body.toString());
      String operationId = requestJson.has("operationId") 
          ? requestJson.get("operationId").asText() 
          : null;
      JsonNode params = requestJson.has("params") 
          ? requestJson.get("params") 
          : mapper.createObjectNode();
      
      if (operationId == null || operationId.isEmpty()) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        mapper.writeValue(resp.getWriter(), mapper.createObjectNode()
            .put("error", "Missing required field: operationId"));
        return;
      }
      
      // Get OperationRegistry from thread-local or execution map
      // Try thread-local first (same thread)
      OperationRegistry registry = currentRegistry.get();
      
      // If not found, try to get from execution ID (cross-thread)
      if (registry == null) {
        String execId = requestJson.has("executionId") 
            ? requestJson.get("executionId").asText() 
            : null;
        if (execId != null) {
          registry = executionRegistryMap.get(execId);
          if (registry != null) {
            log.debug("Found OperationRegistry in execution map for executionId: {}", execId);
          } else {
            log.warn("Execution ID {} not found in execution map. Available IDs: {}", 
                execId, executionRegistryMap.keySet());
          }
        } else {
          log.warn("No executionId provided in request and thread-local registry is null");
        }
      }
      
      if (registry == null) {
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        String errorMsg = "OperationRegistry not available in current execution context. " +
            "Execution ID: " + (requestJson.has("executionId") ? requestJson.get("executionId").asText() : "not provided") +
            ", Available execution IDs: " + executionRegistryMap.keySet();
        log.error(errorMsg);
        mapper.writeValue(resp.getWriter(), mapper.createObjectNode()
            .put("error", errorMsg));
        return;
      }
      
      // Invoke operation - params is already the request body, pass it directly
      JsonNode result = registry.invoke(operationId, params);
      
      // Return result
      resp.setStatus(HttpServletResponse.SC_OK);
      mapper.writeValue(resp.getWriter(), mapper.createObjectNode()
          .set("result", result != null ? result : mapper.createObjectNode()));
      
    } catch (ExecutionPlanException e) {
      log.error("Operation execution failed", e);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      mapper.writeValue(resp.getWriter(), mapper.createObjectNode()
          .put("error", e.getMessage()));
    } catch (Exception e) {
      log.error("Unexpected error in TypeScript bridge", e);
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      mapper.writeValue(resp.getWriter(), mapper.createObjectNode()
          .put("error", "Internal server error: " + e.getMessage()));
    }
  }
}

