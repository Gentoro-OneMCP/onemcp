package com.gentoro.onemcp.indexing.openapi;

import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.handbook.model.agent.Entity;
import com.gentoro.onemcp.handbook.model.agent.EntityOperation;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.*;

/** Converts an API definition (Agent.yaml + OpenAPI) into normalized Knowledge Graph nodes. */
public class OpenApiToNodes {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OpenApiToNodes.class);

  public List<GraphNodeRecord> buildNodes(Api api) {
    List<GraphNodeRecord> nodes = new ArrayList<>();

    OpenAPI openAPI = api.getService().getOpenApi();
    OpenApiSpecParser parser = new OpenApiSpecParser(openAPI);

    // Map Entity name -> openApiTag
    Map<String, String> entityToTag = new HashMap<>();
    Map<String, Set<String>> entityToOps = new HashMap<>();
    for (Entity e : api.getEntities()) {
      if (e.getName() != null) {
        String tag = e.getOpenApiTag() != null ? e.getOpenApiTag() : e.getName();
        entityToTag.put(e.getName(), tag);
        Set<String> ops = new LinkedHashSet<>();
        if (e.getOperations() != null) {
          for (EntityOperation eo : e.getOperations()) {
            if (eo.getKind() != null) ops.add(eo.getKind());
          }
        }
        entityToOps.put(e.getName(), ops);
      }
    }

    // API level documentation node
    String apiDocContent = buildApiDocMarkdown(api, openAPI);
    GraphNodeRecord apiDoc =
        new GraphNodeRecord(key("api", api.getSlug()), KnowledgeNodeType.API_DOCUMENTATION)
            .setApiSlug(api.getSlug())
            .setContent(apiDocContent)
            .setContentFormat("markdown")
            .setEntities(new ArrayList<>(entityToTag.keySet()));
    nodes.add(apiDoc);

    // Operations
    List<OpenApiSpecParser.ParsedOperation> operations = parser.extractOperations();
    for (OpenApiSpecParser.ParsedOperation op : operations) {
      // Determine entities via tag intersection
      List<String> entities = resolveEntitiesForOperation(entityToTag, op.getTags());
      // Determine operation kinds: prefer explicit (from entityToOps for any matched entity), else
      // use the category determined from HTTP method and operation details
      List<String> kinds = resolveOperationKinds(entityToOps, entities, op.getCategory(), op.getOperationId(), op.getMethod(), op.getPath());

      // Operation documentation
      if (op.getDescription() != null || op.getSummary() != null) {
        String md = buildOperationDocMarkdown(api, op);
        nodes.add(
            new GraphNodeRecord(
                    key("opdoc", api.getSlug(), op.getOperationId()),
                    KnowledgeNodeType.API_OPERATION_DOCUMENTATION)
                .setApiSlug(api.getSlug())
                .setOperationId(op.getOperationId())
                .setEntities(entities)
                .setOperations(kinds)
                .setContent(md)
                .setContentFormat("markdown"));
      }

      // Input (request + params) JSON Schema already provided as string in parser
      if (op.getRequestSchema() != null && !op.getRequestSchema().isBlank()) {
        nodes.add(
            new GraphNodeRecord(
                    key("opin", api.getSlug(), op.getOperationId()),
                    KnowledgeNodeType.API_OPERATION_INPUT)
                .setApiSlug(api.getSlug())
                .setOperationId(op.getOperationId())
                .setEntities(entities)
                .setOperations(kinds)
                .setContent(op.getRequestSchema())
                .setContentFormat("json"));
      }
      // Output
      if (op.getResponseSchema() != null && !op.getResponseSchema().isBlank()) {
        nodes.add(
            new GraphNodeRecord(
                    key("opout", api.getSlug(), op.getOperationId()),
                    KnowledgeNodeType.API_OPERATION_OUTPUT)
                .setApiSlug(api.getSlug())
                .setOperationId(op.getOperationId())
                .setEntities(entities)
                .setOperations(kinds)
                .setContent(op.getResponseSchema())
                .setContentFormat("json"));
      }

      // Examples merged into Markdown as requested
      for (OpenApiSpecParser.ParsedExample ex : op.getExamples()) {
        String exMd = buildExampleMarkdown(ex);
        nodes.add(
            new GraphNodeRecord(
                    key("opex", api.getSlug(), op.getOperationId(), ex.getName()),
                    KnowledgeNodeType.API_OPERATION_EXAMPLE)
                .setApiSlug(api.getSlug())
                .setOperationId(op.getOperationId())
                .setEntities(entities)
                .setOperations(kinds)
                .setTitle(ex.getName())
                .setSummary(ex.getSummary())
                .setContent(exMd)
                .setContentFormat("markdown"));
      }
    }

    return nodes;
  }

  private static List<String> resolveEntitiesForOperation(
      Map<String, String> entityToTag, List<String> opTags) {
    Set<String> result = new LinkedHashSet<>();
    if (opTags == null) opTags = List.of();
    for (Map.Entry<String, String> e : entityToTag.entrySet()) {
      for (String tag : opTags) {
        if (tag != null && tag.equalsIgnoreCase(e.getValue())) {
          result.add(e.getKey());
        }
      }
    }
    return new ArrayList<>(result);
  }

  private static List<String> resolveOperationKinds(
      Map<String, Set<String>> entityToOps, List<String> entities, String category,
      String operationId, String method, String path) {
    Set<String> kinds = new LinkedHashSet<>();
    for (String ent : entities) {
      Set<String> ks = entityToOps.get(ent);
      if (ks != null && !ks.isEmpty()) kinds.addAll(ks);
    }
    if (kinds.isEmpty()) {
      // Use the category determined from HTTP method and operation details
      // (Retrieve, Compute, Create, Update, or Delete)
      if (category != null && !category.isBlank()) {
        kinds.add(category);
      } else {
        // Log when category is missing instead of falling back
        log.warn(
            "Operation category is missing for operation '{}' ({} {}). No operation kinds will be assigned.",
            operationId != null ? operationId : "unknown",
            method != null ? method : "unknown",
            path != null ? path : "unknown");
      }
    }
    return new ArrayList<>(kinds);
  }

  private static String buildApiDocMarkdown(Api api, OpenAPI openAPI) {
    StringBuilder sb = new StringBuilder();
    sb.append("# ").append(api.getName() != null ? api.getName() : api.getSlug()).append("\n\n");
    if (api.getDescription() != null) sb.append(api.getDescription()).append("\n\n");
    if (openAPI.getInfo() != null && openAPI.getInfo().getDescription() != null) {
      sb.append(openAPI.getInfo().getDescription()).append("\n\n");
    }
    return sb.toString().trim();
  }

  private static String buildOperationDocMarkdown(Api api, OpenApiSpecParser.ParsedOperation op) {
    StringBuilder sb = new StringBuilder();
    sb.append("### ").append(op.getOperationId()).append("\n\n");
    if (op.getSummary() != null) sb.append(op.getSummary()).append("\n\n");
    if (op.getDescription() != null) sb.append(op.getDescription()).append("\n\n");
    sb.append("- Method: ").append(op.getMethod()).append("\n");
    sb.append("- Path: ").append(op.getPath()).append("\n");
    if (op.getTags() != null && !op.getTags().isEmpty()) {
      sb.append("- Tags: ").append(String.join(", ", op.getTags())).append("\n");
    }
    return sb.toString().trim();
  }

  private static String buildExampleMarkdown(OpenApiSpecParser.ParsedExample ex) {
    StringBuilder sb = new StringBuilder();
    if (ex.getName() != null) sb.append("#### ").append(ex.getName()).append("\n\n");
    if (ex.getSummary() != null) sb.append(ex.getSummary()).append("\n\n");
    if (ex.getDescription() != null) sb.append(ex.getDescription()).append("\n\n");
    if (ex.getRequestBody() != null && !ex.getRequestBody().isBlank()) {
      sb.append("Input\n\n");
      sb.append("```json\n").append(ex.getRequestBody()).append("\n```\n\n");
    }
    if (ex.getResponseBody() != null && !ex.getResponseBody().isBlank()) {
      sb.append("Output");
      if (ex.getResponseStatus() != null)
        sb.append(" (status ").append(ex.getResponseStatus()).append(")");
      sb.append("\n\n");
      sb.append("```json\n").append(ex.getResponseBody()).append("\n```\n\n");
    }
    return sb.toString().trim();
  }

  private static String key(String... parts) {
    return String.join("|", parts);
  }
}
