package com.gentoro.onemcp.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.util.HashMap;
import java.util.Map;

public class OpenApiLoader {

  public static OpenAPI load(String specPath) {
    return new OpenAPIV3Parser().read(specPath);
  }

  public static Map<String, EndpointInvoker> buildInvokers(OpenAPI openAPI, String baseUrl) {
    Map<String, EndpointInvoker> invokers = new HashMap<>();

    if (openAPI.getPaths() == null) return invokers;

    openAPI
        .getPaths()
        .forEach(
            (path, pathItem) -> {
              Map<PathItem.HttpMethod, Operation> ops = pathItem.readOperationsMap();
              ops.forEach(
                  (httpMethod, operation) -> {
                    String operationId = operation.getOperationId();
                    if (operationId == null) return;

                    invokers.put(
                        operationId,
                        new EndpointInvoker(baseUrl, path, httpMethod.name(), operation));
                  });
            });

    return invokers;
  }
}
