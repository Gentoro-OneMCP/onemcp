package com.gentoro.onemcp.indexing.openapi;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.handbook.model.agent.Entity;
import com.gentoro.onemcp.handbook.model.agent.EntityOperation;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.tags.Tag;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OpenApiToNodesTest {

  @Test
  @DisplayName("buildNodes creates API doc, operation docs, input/output and example nodes")
  void buildNodesEndToEnd() throws Exception {
    // Temp handbook structure with apis ref file so Service(Api) constructor doesn't fail
    Path tmp = Files.createTempDirectory("hb_oapi_");
    Path apis = Files.createDirectories(tmp.resolve("apis"));
    Path refFile = apis.resolve("svc.yaml");
    Files.writeString(refFile, "openapi: 3.0.0\ninfo: {title: T, version: v}\n");

    // Minimal Handbook stub
    Handbook hb =
        new Handbook() {
          @Override
          public Path location() {
            return tmp;
          }

          @Override
          public com.gentoro.onemcp.handbook.model.agent.Agent agent() {
            return new com.gentoro.onemcp.handbook.model.agent.Agent();
          }

          @Override
          public java.util.Optional<Api> optionalApi(String slug) {
            return java.util.Optional.empty();
          }

          @Override
          public Api api(String slug) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Map<String, Api> apis() {
            return Map.of();
          }

          @Override
          public Map<String, com.gentoro.onemcp.handbook.model.regression.RegressionSuite>
              regressionSuites() {
            return Map.of();
          }

          @Override
          public java.util.Optional<com.gentoro.onemcp.handbook.model.regression.RegressionSuite>
              optionalRegressionSuite(String relativePath) {
            return java.util.Optional.empty();
          }

          @Override
          public com.gentoro.onemcp.handbook.model.regression.RegressionSuite regressionSuite(
              String relativePath) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Map<String, String> documentation() {
            return Map.of();
          }

          @Override
          public java.util.Optional<String> optionalDocumentation(String relativePath) {
            return java.util.Optional.empty();
          }

          @Override
          public String documentation(String relativePath) {
            throw new UnsupportedOperationException();
          }

          @Override
          public com.gentoro.onemcp.OneMcp oneMcp() {
            return null;
          }

          @Override
          public String name() {
            return "test";
          }
        };

    Api api = new Api();
    api.setSlug("orders");
    api.setName("Orders API");
    api.setRef("svc.yaml");
    Entity ent = new Entity();
    ent.setName("Order");
    ent.setOpenApiTag("order");
    List<EntityOperation> ops = new ArrayList<>();
    EntityOperation eo = new EntityOperation();
    eo.setKind("Retrieve");
    ops.add(eo);
    ent.setOperations(ops);
    api.setEntities(List.of(ent));
    api.bindHandbook(hb); // creates a Service and binds

    // Build an OpenAPI with one GET operation tagged "order"
    OpenAPI oapi = new OpenAPI();
    oapi.setTags(List.of(new Tag().name("order").description("Order operations")));
    Operation op = new Operation();
    op.setOperationId("getById");
    op.setSummary("Get order");
    op.setDescription("Retrieve order by ID");
    op.setTags(List.of("order"));
    // Request schema
    RequestBody rb = new RequestBody();
    rb.setContent(
        new Content()
            .addMediaType(
                "application/json", new MediaType().schema(new Schema<>().type("object"))));
    op.setRequestBody(rb);
    // Response schema
    ApiResponses responses = new ApiResponses();
    responses.addApiResponse(
        "200",
        new ApiResponse()
            .content(
                new Content()
                    .addMediaType(
                        "application/json",
                        new MediaType().schema(new Schema<>().type("object")))));
    op.setResponses(responses);

    PathItem pathItem = new PathItem();
    pathItem.setGet(op);
    oapi.path("/orders/{id}", pathItem);

    // Inject OpenAPI into the bound Service
    api.getService().setOpenApi(oapi);

    List<GraphNodeRecord> nodes = new OpenApiToNodes().buildNodes(api);
    assertFalse(nodes.isEmpty());
    assertTrue(
        nodes.stream().anyMatch(n -> n.getNodeType() == KnowledgeNodeType.API_DOCUMENTATION));
    assertTrue(
        nodes.stream()
            .anyMatch(n -> n.getNodeType() == KnowledgeNodeType.API_OPERATION_DOCUMENTATION));
    assertTrue(
        nodes.stream().anyMatch(n -> n.getNodeType() == KnowledgeNodeType.API_OPERATION_INPUT));
    assertTrue(
        nodes.stream().anyMatch(n -> n.getNodeType() == KnowledgeNodeType.API_OPERATION_OUTPUT));
    // All nodes should carry correct apiSlug
    assertTrue(
        nodes.stream()
            .allMatch(
                n ->
                    "orders".equals(n.getApiSlug())
                        || n.getNodeType() == KnowledgeNodeType.DOCS_CHUNK));
    // Formats
    assertTrue(
        nodes.stream()
            .filter(
                n ->
                    n.getNodeType().name().contains("INPUT")
                        || n.getNodeType().name().contains("OUTPUT"))
            .allMatch(n -> "json".equals(n.getContentFormat())));
  }
}
