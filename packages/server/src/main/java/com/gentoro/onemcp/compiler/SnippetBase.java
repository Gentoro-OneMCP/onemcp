package com.gentoro.onemcp.compiler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.context.Operation;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.exception.NetworkException;
import com.gentoro.onemcp.exception.OneMcpException;
import com.gentoro.onemcp.http.OkHttpFactory;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.orchestrator.OrchestratorContext;
import com.gentoro.onemcp.utility.JacksonUtility;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public abstract class SnippetBase {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(JavaSnippetCompiler.class);

  private OrchestratorContext context;

  public void setContext(OrchestratorContext context) {
    this.context = context;
  }

  public ObjectMapper getObjectMapper() {
    return JacksonUtility.getJsonMapper();
  }

  public ValueStore getValueStore() {
    return context.memory();
  }

  public OrchestratorContext getContext() {
    return context;
  }

  private Service getService(String serviceSlug) {
    log.trace("Getting service {}", serviceSlug);
    return context.knowledgeBase().services().stream()
        .filter(s -> s.getSlug().equals(serviceSlug))
        .findFirst()
        .orElseThrow(
            () -> {
              log.error(
                  "Service {} not found. Here is the complete list of available services: {}",
                  serviceSlug,
                  Objects.requireNonNullElse(
                          context.knowledgeBase().services(), Collections.<Service>emptyList())
                      .stream()
                      .map(Service::getSlug)
                      .toList());
              return new com.gentoro.onemcp.exception.NotFoundException(
                  "Service not found: " + serviceSlug);
            });
  }

  public String getBaseUrl(Service service) {
    OpenAPI definition = service.definition(context.knowledgeBase().handbookPath());
    return definition.getServers().stream()
        .filter(
            s ->
                s != null
                    && s.getUrl() != null
                    && (s.getUrl().startsWith("http") || s.getUrl().startsWith("https")))
        .map(Server::getUrl)
        .findFirst()
        .orElseThrow(
            () -> {
              log.error("No base URL found for service {}", service.getSlug());
              return new com.gentoro.onemcp.exception.NotFoundException(
                  "No base URL found for service " + service.getSlug());
            });
  }

  public OkHttpClient getServiceClient(String serviceSlug) {
    return OkHttpFactory.create(getBaseUrl(getService(serviceSlug)));
  }

  public Request.Builder initRequest(String serviceSlug, String operation) {
    log.trace("Initializing request for service {} and operation {}", serviceSlug, operation);
    Service service = getService(serviceSlug);
    Operation serviceOperation =
        service.getOperations().stream()
            .filter(o -> o.getOperation().equals(operation))
            .findFirst()
            .orElseThrow(
                () -> {
                  log.error("Operation {} not found for service {}", operation, serviceSlug);
                  return new com.gentoro.onemcp.exception.NotFoundException(
                      "The service (%s) was properly located, but there are no operations identified by %s"
                          .formatted(serviceSlug, operation));
                });

    return new Request.Builder()
        .url("%s%s".formatted(getBaseUrl(getService(serviceSlug)), serviceOperation.getPath()));
  }

  public Response executeRequest(String serviceSlug, Request.Builder requestBuilder) {
    long start = System.currentTimeMillis();
    log.trace(
        "Executing request for service {} and operation {}",
        serviceSlug,
        requestBuilder.build().url());
    try {
      Response response = getServiceClient(serviceSlug).newCall(requestBuilder.build()).execute();
      if (response.isSuccessful()) {
        log.trace("Request completed successfully in ({}ms)", (System.currentTimeMillis() - start));
        return response;
      } else {
        throw new NetworkException("Request failed with status code %d".formatted(response.code()));
      }
    } catch (OneMcpException e) {
      throw e;
    } catch (Exception e) {
      throw new NetworkException("Request could not be executed properly.", e);
    }
  }

  public abstract Set<String> run();
}
