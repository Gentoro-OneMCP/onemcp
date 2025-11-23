package com.gentoro.onemcp.orchestrator.progress;

import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.Map;
import java.util.Objects;

/**
 * Progress sink that streams progress updates to the active MCP session using the official {@link
 * McpSyncServerExchange} API.
 *
 * <p>How it works:
 *
 * <ul>
 *   <li>On each eligible update, it invokes {@link
 *       McpSyncServerExchange#progressNotification(io.modelcontextprotocol.spec.McpSchema.ProgressNotification)}
 *       with a {@link McpSchema.ProgressNotification} built from the current state.
 *   <li>{@code completed} and {@code total} are sent as {@code double} values to match the MCP
 *       schema. The original values are still tracked as {@code long} internally.
 *   <li>The notification includes a stable, structured {@code payload} produced by {@link
 *       LoggingProgressSink#createPayload(String, String, long, long, String, java.util.Map,
 *       String)}, ensuring parity with {@link LoggingProgressSink} for downstream consumers.
 *   <li>Rate limiting is identical to {@link ProgressRateLimiter} behavior in the base class to
 *       avoid excessive emissions.
 *   <li>For local observability and backward compatibility, the same message is also emitted to the
 *       application logs via {@link LoggingProgressSink#emit(String, String, long, long, String,
 *       java.util.Map, String)}.
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>This implementation no longer relies on reflective resolution of notification emitters; it
 *       uses the strongly-typed {@link McpSyncServerExchange} provided by the SDK.
 *   <li>A non-null {@code progressToken} provided by the caller is required and is forwarded to the
 *       MCP client as the {@code id} of the {@link McpSchema.ProgressNotification}.
 * </ul>
 */
public class McpProgressSink extends LoggingProgressSink {

  private final McpSyncServerExchange exchange;
  private final Object progressToken;

  /**
   * Creates a sink that mirrors progress both to the MCP exchange and to the local logger.
   *
   * @param logger the logger used by the base {@link LoggingProgressSink}
   * @param minIntervalMs minimal time between two emitted updates (rate limiting)
   * @param minDelta minimal required delta for {@code completed} to emit (rate limiting)
   * @param exchange the active {@link McpSyncServerExchange} used to send progress notifications
   * @param progressToken token that identifies the logical progress stream on the client side
   */
  public McpProgressSink(
      org.slf4j.Logger logger,
      long minIntervalMs,
      long minDelta,
      McpSyncServerExchange exchange,
      Object progressToken) {
    super(logger, minIntervalMs, minDelta);
    this.exchange = Objects.requireNonNull(exchange, "exchange");
    this.progressToken = Objects.requireNonNull(progressToken, "progressToken");
  }

  @Override
  void emit(
      String id,
      String label,
      long completed,
      long total,
      String message,
      Map<String, Object> attrs,
      String status) {
    Map<String, Object> payload =
        createPayload(id, label, completed, total, message, attrs, status);
    exchange.progressNotification(
        new McpSchema.ProgressNotification(
            String.valueOf(progressToken),
            Long.valueOf(completed).doubleValue(),
            Long.valueOf(total).doubleValue(),
            label,
            payload));

    // Also log locally (keeps backward compatibility and observability)
    super.emit(id, label, completed, total, message, attrs, status);
  }
}
