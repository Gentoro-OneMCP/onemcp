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
    if (apiDef != null && apiDef.getBaseUrls() != null && !apiDef.getBaseUrls().isEmpty()) {
      baseUrl.set(apiDef.getBaseUrls().getFirst());
    }

    if (baseUrl.get() != null && !baseUrl.get().isEmpty()) {
      openApi.getServers().stream()
          .filter(s -> s != null && s.getUrl() != null && !s.getUrl().isEmpty())
          .findFirst()
          .ifPresent(s -> baseUrl.set(s.getUrl()));
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
