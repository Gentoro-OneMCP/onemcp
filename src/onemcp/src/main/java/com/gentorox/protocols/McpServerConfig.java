package com.gentorox.protocols;

import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.WebFluxStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.server.RouterFunction;
import reactor.core.publisher.Mono;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * MCP Async Server configuration for the HTTP Streamable transport (Spring WebFlux).
 * <p>
 * This configuration wires:
 * - McpJsonMapper for JSON serialization
 * - WebFluxStreamableServerTransportProvider to expose the MCP endpoint over HTTP(S)
 * - RouterFunction to register transport routes
 * - McpAsyncServer defining server capabilities and tools
 * - Async tool specification that bridges MCP tool calls to the internal Orchestrator
 * <p>
 * The transport endpoint can be configured via application properties:
 * - agentProtocol.mcp.config.disallowDelete (default: false)
 * - agentProtocol.mcp.config.messageEndpoint (default: /mcp)
 * The final URL is built from the server port and optional servlet context path.
 */
@Configuration
public class McpServerConfig {
  private static final Logger LOG = LoggerFactory.getLogger(McpServerConfig.class);
  /**
   * Provides the default MCP JSON mapper used for serializing messages on the wire.
   */
  @Bean
  McpJsonMapper mcpJsonMapper() {
    return McpJsonMapper.createDefault();
  }

  /**
   * Configures the HTTP Streamable transport provider for MCP using Spring WebFlux.
   * The endpoint is exposed at the configured messageEndpoint and respects the server port and context path.
   */
  @Bean
  WebFluxStreamableServerTransportProvider webFluxStreamableServerTransportProvider(McpJsonMapper jsonMapper,
                                                              ServerProperties serverProperties,
                                                              @Value("${server.servlet.context-path:}") String contextPath,
                                                              @Value("${agentProtocol.mcp.config.disallowDelete:false}") boolean disallowDelete,
                                                              @Value("${agentProtocol.mcp.config.messageEndpoint:/mcp}") String messageEndpoint) {
   WebFluxStreamableServerTransportProvider provider = WebFluxStreamableServerTransportProvider.builder()
       .jsonMapper(jsonMapper)
       .disallowDelete(disallowDelete)
       .messageEndpoint(messageEndpoint)
       .build();

    // build full URL
    String host = "localhost";
    int port = (serverProperties.getPort() != null) ? serverProperties.getPort() : 8080;
    String ctx = (contextPath != null) ? contextPath : "";
    if (!ctx.startsWith("/")) ctx = "/" + ctx;
    if (ctx.equals("/")) ctx = ""; // avoid double slash

    // ensure proper formatting
    String normalizedEndpoint = messageEndpoint.startsWith("/") ? messageEndpoint : "/" + messageEndpoint;
    String fullUrl = String.format("http://%s:%d%s%s", host, port, ctx, normalizedEndpoint);

    LOG.info("Created MCP server transport provider (HTTP Streamable)");
    LOG.info("MCP URL: {}", fullUrl);

   return provider;
  }

  /**
   * Exposes the MCP transport routes (HTTP endpoints) as a Spring WebFlux RouterFunction.
   */
  @Bean
  RouterFunction<?> mcpRouterFunction(WebFluxStreamableServerTransportProvider transportProvider) {
    return transportProvider.getRouterFunction();
  }

  /**
   * Builds the MCP Async Server with declared capabilities and the configured tool.
   */
  @Bean
  McpAsyncServer mcpServer(WebFluxStreamableServerTransportProvider transportProvider, McpServerFeatures.AsyncToolSpecification singleToolSpec) {
    var capabilities = McpSchema.ServerCapabilities.builder()
        .resources(false, true)
        .tools(true)
        .prompts(false)
        .logging()
        .build();

    return McpServer.async(transportProvider)
        .capabilities(capabilities)
        .tools(singleToolSpec)
        .build();
  }

  /**
   * Defines a single asynchronous tool exposed by the MCP server. The tool proxies calls
   * to the internal Orchestrator using a user-provided prompt plus optional options map.
   */
  @Bean
  McpServerFeatures.AsyncToolSpecification asyncToolSpecification(McpJsonMapper jsonMapper, com.gentorox.services.agent.Orchestrator orchestrator) {
    return new McpServerFeatures.AsyncToolSpecification (
        McpSchema.Tool.builder()
            .name("gentoro.run")
            .description(
            """
            Run an instruction using the Gentoro Agent. {prompt, options}.
            Example:
              {
                "prompt": "Summarize the latest release notes for the Gentoro API and highlight any breaking changes.",
                "options": {
                  "headers": {
                    "Authorization": "Bearer sk-abc123...",
                    "Content-Type": "application/json"
                  },
                  "telemetry": {
                    "rootSpanId": "op-20151-874215"
                  },
                  ...
                }
              }
            """)
            .inputSchema(
                new McpSchema.JsonSchema(
                    "object",
                    Map.of(
                        "prompt", Map.of(
                            "type", "string",
                            "description", "Defines the user/client intent in natural language"
                        ),
                        "options", Map.of(
                            "type", "object",
                            "description", "Collection of additional settings to be used during the request, such as headers or auth"
                        )
                    ),
                    List.of("prompt"), // prompt is required
                    false,
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            )
            .build(),
        null,
        (exchange, request) -> {
          LOG.info("Received MCP tool call: {}", request.name());
          try {
            final Map<String, Object> args = request.arguments();
            if (args == null) {
              return Mono.just(new McpSchema.CallToolResult("Missing arguments", true));
            }

            // 1) Required: prompt
            final Object promptObj = args.get("prompt");
            if (!(promptObj instanceof String) || ((String) promptObj).isBlank()) {
              return Mono.just(new McpSchema.CallToolResult("`prompt` (string) is required", true));
            }
            final String prompt = (String) promptObj;

            // 2) Optional: options (object)
            final Object optionsObj = args.get("options");
            final Map<String, Object> options = asObjectMap(optionsObj); // may be null

            LOG.info("Calling orchestrator with prompt: {}", prompt);
            InferenceResponse resp = orchestrator.run(List.of(new InferenceRequest.Message("user", prompt)), options == null ? Map.of() : options);
            Assert.notNull(resp, "Orchestrator response must not be null");
            LOG.info("Orchestrator returned response with traceId: {}", resp.providerTraceId());

            return Mono.just(
                new McpSchema.CallToolResult(
                    jsonMapper.writeValueAsString(
                        Map.of("content", resp.content(), "traceId", resp.providerTraceId())),
                    false));
          } catch (Exception e) {
            return Mono.just(new McpSchema.CallToolResult("Failed while executing tool call: " + request.name(), true));
          }
        });
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asObjectMap(Object v) {
    if (v instanceof Map<?, ?> m) {
      // Best-effort unchecked cast to Map<String, Object>
      return (Map<String, Object>) m;
    }
    return null;
  }
}
