package com.gentoro.onemcp.handbook.model.agent;

import com.gentoro.onemcp.exception.ConfigException;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.openapi.EndpointInvoker;
import com.gentoro.onemcp.openapi.OpenApiLoader;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.servers.Server;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Service {
  private OpenAPI openApi;
  private Map<String, EndpointInvoker> invokers;
  private transient Api apiDef;

  public Service(Handbook handbook, Path apiDefPath) {

    Path apisPath = handbook.location().resolve("./apis");
    if (!apiDefPath.startsWith(apisPath)) {
      throw new ConfigException("Service definition must be located in the apis folder");
    }

    this.initialize(apiDefPath);

    final Api api = new Api();
    api.setRef(apisPath.relativize(apiDefPath).toString());
    api.setName(
        StringUtility.requireNonEmpty(
            getOpenApi().getInfo().getTitle(), apiDefPath.getFileName().toString()));
    api.setSlug(StringUtility.sanitize(api.getName()).toLowerCase());
    api.setBaseUrls(getOpenApi().getServers().stream().map(Server::getUrl).toList());
    api.setDescription(getOpenApi().getInfo().getDescription());
    getOpenApi()
        .getTags()
        .forEach(
            tag -> {
              final Entity entity = new Entity();
              entity.setName(tag.getName());
              entity.setDescription(tag.getDescription());
              entity.setOpenApiTag(tag.getName());
              getOpenApi()
                  .getPaths()
                  .forEach(
                      (path, pathItem) -> {
                        pathItem.readOperationsMap().entrySet().stream()
                            .filter(entry -> entry.getValue().getTags().contains(tag.getName()))
                            .forEach(
                                entry -> {
                                  String operationKind = httpMethodAsOperationKind(entry.getKey());
                                  if (operationKind != null) {
                                    EntityOperation entityOperation = new EntityOperation();
                                    entityOperation.setKind(operationKind);
                                    entityOperation.setDescription(
                                        entry.getValue().getDescription());
                                    entity.addOperation(entityOperation);
                                  }
                                });
                      });
              api.addEntity(entity);
            });
    this.apiDef = api;
    this.apiDef.bindHandbook(handbook, this);
  }

  private String httpMethodAsOperationKind(PathItem.HttpMethod httpMethod) {
    return switch (httpMethod) {
      case GET -> "retrieve";
      case POST -> "create";
      case PUT -> "update";
      case DELETE -> "delete";
      default -> null;
    };
  }

  public Service(Api apiDef) {
    this.apiDef = apiDef;
    Path apiDefPath = apiDef.getHandbook().location().resolve("./apis/" + apiDef.getRef());
    if (!Files.exists(apiDefPath)) {
      throw new ConfigException("Service definition not found: " + apiDef.getRef());
    }
    this.initialize(apiDefPath);
  }

  private void initialize(Path apiDefPath) {
    openApi = OpenApiLoader.load(apiDefPath.toString());

    AtomicReference<String> baseUrl = new AtomicReference<>();
    
    // First, try to get baseUrl from OpenAPI spec servers (preferred)
    openApi.getServers().stream()
        .filter(s -> s != null && s.getUrl() != null && !s.getUrl().isEmpty())
        .findFirst()
        .ifPresent(s -> baseUrl.set(s.getUrl()));
    
    // If no server URL in OpenAPI spec, fall back to apiDef baseUrls
    if (baseUrl.get() == null || baseUrl.get().isEmpty()) {
      if (apiDef != null && apiDef.getBaseUrls() != null && !apiDef.getBaseUrls().isEmpty()) {
        baseUrl.set(apiDef.getBaseUrls().getFirst());
      }
    }

    invokers = OpenApiLoader.buildInvokers(openApi, baseUrl.get());
  }

  public OpenAPI getOpenApi() {
    return openApi;
  }

  public void setOpenApi(OpenAPI openApi) {
    this.openApi = openApi;
  }

  public Map<String, EndpointInvoker> getInvokers() {
    return invokers;
  }

  public void setInvokers(Map<String, EndpointInvoker> invokers) {
    this.invokers = invokers;
  }

  public Api getApiDef() {
    return apiDef;
  }
}
