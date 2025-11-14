package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.exception.StateException;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.Map;

public class OpenApiProxyImpl implements OpenApiProxy {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OpenApiProxyImpl.class);
  private final Map<String, EndpointInvoker> invokers;

  public OpenApiProxyImpl(OpenAPI openAPI, String baseUrl) {
    this.invokers = OpenApiLoader.buildInvokers(openAPI, baseUrl);
  }

  @Override
  public JsonNode invoke(String operationId, JsonNode input) throws Exception {
    EndpointInvoker invoker = invokers.get(operationId);
    if (invoker == null) {
      throw new StateException("Unknown operationId: " + operationId);
    }
    log.trace("Invoking operation {}", operationId);
    return invoker.invoke(input);
  }
}
