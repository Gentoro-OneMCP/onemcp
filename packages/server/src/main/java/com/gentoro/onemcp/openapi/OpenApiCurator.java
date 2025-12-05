package com.gentoro.onemcp.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import java.util.*;

public class OpenApiCurator {

  public static Map<String, Object> entityGenerationContext(
      OpenAPI openAPI, boolean includeSchema) {

    Map<String, Object> root = new LinkedHashMap<>();

    // ---- API general documentation ----
    String apiDescription = null;
    if (openAPI.getInfo() != null) {
      apiDescription = openAPI.getInfo().getDescription();
    }
    root.put("documentation", apiDescription);

    // ---- Paths ----
    Map<String, Object> curatedPaths = new LinkedHashMap<>();

    if (openAPI.getPaths() != null) {
      openAPI
          .getPaths()
          .forEach(
              (path, pathItem) -> {
                List<Map<String, Object>> operationsList = new ArrayList<>();

                // For every HTTP method / operation defined under the path
                pathItem
                    .readOperationsMap()
                    .forEach(
                        (httpMethod, operation) -> {
                          Map<String, Object> opEntry = new LinkedHashMap<>();

                          opEntry.put("httpMethod", httpMethod.toString());
                          opEntry.put("operationId", operation.getOperationId());
                          opEntry.put("documentation", getOperationDocumentation(operation));
                          if (includeSchema) {
                            if (Objects.nonNull(operation.getParameters())
                                && !operation.getParameters().isEmpty()) {
                              opEntry.put(
                                  "parameters",
                                  operation.getParameters().stream()
                                      .map(
                                          op ->
                                              Map.of(
                                                  "name", op.getName(),
                                                  "description", op.getDescription(),
                                                  "in", op.getIn(),
                                                  "schema", op.getSchema()))
                                      .toList());
                            }

                            if (Objects.nonNull(operation.getRequestBody())) {
                              opEntry.put(
                                  "requestBody",
                                  operation.getRequestBody().getContent().entrySet().stream()
                                      .map(
                                          content ->
                                              Map.of(
                                                  "contentType",
                                                  content.getKey(),
                                                  "schema",
                                                  content.getValue().getSchema()))
                                      .toList());
                            }

                            opEntry.put(
                                "responses",
                                operation.getResponses().entrySet().stream()
                                    .filter(
                                        e ->
                                            Objects.nonNull(e.getValue())
                                                && Objects.nonNull(e.getValue().getContent()))
                                    .map(
                                        (e) ->
                                            Map.of(
                                                "response", e.getKey(),
                                                "description", e.getValue().getDescription(),
                                                "contents",
                                                    Objects.isNull(e.getValue().getContent())
                                                        ? null
                                                        : e
                                                            .getValue()
                                                            .getContent()
                                                            .entrySet()
                                                            .stream()
                                                            .map(
                                                                content ->
                                                                    Map.of(
                                                                        "contentType",
                                                                        content.getKey(),
                                                                        "schema",
                                                                        content
                                                                            .getValue()
                                                                            .getSchema()))
                                                            .toList()))
                                    .toList());
                          }
                          operationsList.add(opEntry);
                        });

                curatedPaths.put(path, operationsList);
              });
    }

    root.put("paths", curatedPaths);

    return root;
  }

  /** Prefer summary; fallback to description */
  private static String getOperationDocumentation(Operation operation) {
    if (operation.getSummary() != null) return operation.getSummary();
    return operation.getDescription();
  }
}
