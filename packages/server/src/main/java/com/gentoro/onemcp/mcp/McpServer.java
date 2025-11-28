package com.gentoro.onemcp.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.orchestrator.progress.McpProgressSink;
import com.gentoro.onemcp.orchestrator.progress.NoOpProgressSink;
import com.gentoro.onemcp.orchestrator.progress.ProgressSink;
import com.gentoro.onemcp.utility.JacksonUtility;
import io.modelcontextprotocol.json.jackson.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.configuration2.Configuration;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;

/**
 * Jetty-based MCP server packaged as a lifecycle-managed service.
 *
 * <p>This class embeds Jetty 12 (Jakarta Servlet 6) and the official MCP SDK to expose a Streamable
 * HTTP endpoint. It is designed as a reusable service with explicit start/stop methods and a JVM
 * shutdown hook to ensure resources are released on application exit.
 *
 * <p>Configuration is provided via Apache Commons Configuration (see {@link
 * org.apache.commons.configuration2.Configuration}). The following keys are supported:
 *
 * <ul>
 *   <li><b>mcp.http.port</b> (int) – TCP port to bind; default: 8080
 *   <li><b>mcp.http.endpoint</b> (string) – servlet path; default: "/mcp"
 *   <li><b>mcp.http.disallowDelete</b> (boolean) – reject HTTP DELETE; default: false
 *   <li><b>mcp.http.keepAlive</b> (string) – ISO-8601 duration (e.g. "PT30S"); default: PT0S
 *       (disabled)
 *   <li><b>mcp.server.name</b> (string) – server name reported to clients; default:
 *       "java-orchestrator-mcp"
 *   <li><b>mcp.server.version</b> (string) – server version reported to clients; default: "1.0.0"
 * </ul>
 *
 * <p>Example YAML (src/main/resources/application.yaml):
 *
 * <pre>
 * mcp:
 *   http:
 *     port: 8080
 *     endpoint: "/mcp"
 *     disallowDelete: false
 *     keepAlive: "PT0S"
 *   server:
 *     name: "java-orchestrator-mcp"
 *     version: "1.0.0"
 * </pre>
 *
 * <p>Usage:
 *
 * <pre>{@code
 * Configuration cfg = ConfigurationProvider.getInstance().config();
 * try (McpServer server = new McpServer(cfg)) {
 *   server.start();
 *   server.join();
 * }
 * }</pre>
 */
public class McpServer implements AutoCloseable {

  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(McpServer.class);

  private final OneMcp oneMcp;
  private HttpServletStreamableServerTransportProvider servletTransport;
  private McpSyncServer mcpServer;

  public McpServer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Register the MCP servlet on the provided Jetty {@link Server} without managing its lifecycle.
   */
  public void register() {
    String endpoint =
        normalizeEndpoint(oneMcp.configuration().getString("http.mcp.endpoint", "/mcp"));
    boolean disallowDelete = oneMcp.configuration().getBoolean("http.mcp.disallow-delete", false);
    Duration keepAlive = resolveKeepAlive(oneMcp.configuration());

    String serverName = oneMcp.configuration().getString("http.mcp.server.name", "onemcp");
    String serverVersion = oneMcp.configuration().getString("http.mcp.server.version", "1.0.0");

    // MCP JSON codec and servlet-based transport
    var json = new JacksonMcpJsonMapper(new ObjectMapper());
    servletTransport =
        HttpServletStreamableServerTransportProvider.builder()
            .jsonMapper(json)
            .mcpEndpoint(endpoint)
            .disallowDelete(disallowDelete)
            // .keepAliveInterval(keepAlive)
            .build();

    // Build the MCP server with capabilities and example tool
    mcpServer =
        io.modelcontextprotocol.server.McpServer.sync(servletTransport)
            .serverInfo(serverName, serverVersion)
            .capabilities(McpSchema.ServerCapabilities.builder().tools(true).logging().build())
            .tools(
                McpServerFeatures.SyncToolSpecification.builder()
                    .tool(
                        McpSchema.Tool.builder()
                            .name(
                                oneMcp
                                    .configuration()
                                    .getString("http.mcp.tool.name", "onemcp.run"))
                            .description(
                                oneMcp
                                    .configuration()
                                    .getString(
                                        "http.mcp.tool.description",
                                        "OneMCP entry point, express your request using Natural Language."))
                            .inputSchema(
                                new McpSchema.JsonSchema(
                                    "object",
                                    Map.of("prompt", Map.of("type", "string")),
                                    List.of("prompt"),
                                    false,
                                    Collections.emptyMap(),
                                    Collections.emptyMap()))
                            .build())
                    .callHandler(
                        (srv, request) -> {
                          try {
                            Object progressToken =
                                Objects.requireNonNullElse(
                                        request.meta(), Collections.<String, Object>emptyMap())
                                    .get("progressToken");
                            boolean progressRequested = progressToken != null;

                            // Progress configuration (optional)
                            boolean progressEnabled =
                                oneMcp
                                    .configuration()
                                    .getBoolean("http.mcp.progress.enabled", true);
                            long minIntervalMs =
                                oneMcp
                                    .configuration()
                                    .getLong("http.mcp.progress.min-interval-ms", 300L);
                            long minDelta =
                                oneMcp.configuration().getLong("http.mcp.progress.min-delta", 1L);
                            ProgressSink sink;
                            if (progressEnabled && progressRequested) {
                              sink =
                                  new McpProgressSink(
                                      log, // mirror to the same category for diagnostics
                                      minIntervalMs,
                                      minDelta,
                                      srv,
                                      progressToken);
                            } else {
                              sink = new NoOpProgressSink();
                            }

                            var result =
                                oneMcp
                                    .orchestrator()
                                    .handlePrompt(
                                        request.arguments().get("prompt").toString(), sink);

                            return new McpSchema.CallToolResult(
                                JacksonUtility.toJson(result), false);
                          } catch (Exception e) {
                            log.error("Failed to handle MCP tool request", e);
                            return new McpSchema.CallToolResult(
                                Objects.requireNonNullElse(
                                    e.getMessage(), ExceptionUtil.formatCompactStackTrace(e)),
                                true);
                          }
                        })
                    .build())
            .build();

    // Mount servlet on the shared context handler
    oneMcp
        .httpServer()
        .getContextHandler()
        .addServlet(new ServletHolder(servletTransport), endpoint);

    log.info(
        "MCP servlet registered at http://localhost:{}{}", oneMcp.httpServer().getPort(), endpoint);
  }

  /** Clean up MCP transport/resources. */
  @Override
  public void close() {
    if (servletTransport != null) {
      try {
        servletTransport.destroy();
      } finally {
        servletTransport = null;
      }
    }
    mcpServer = null;
  }

  private static String normalizeEndpoint(String endpoint) {
    if (endpoint == null || endpoint.isBlank()) return "/mcp";
    return endpoint.startsWith("/") ? endpoint : "/" + endpoint;
  }

  private static Duration resolveKeepAlive(Configuration cfg) {
    String d = cfg.getString("http.mcp.keep-alive", null);
    if (d != null && !d.isBlank()) {
      try {
        return Duration.parse(d.trim());
      } catch (Exception ignored) {
        // fall back to seconds key
      }
    }
    int seconds = cfg.getInt("http.mcp.keep-alive-seconds", 0);
    if (seconds > 0) return Duration.ofSeconds(seconds);
    return Duration.ZERO;
  }
}
