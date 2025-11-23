package com.gentoro.onemcp.orchestrator.progress;

import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Progress sink that emits structured JSON messages to the application logs.
 *
 * <p>This is a transport-agnostic fallback that allows MCP clients (or any log consumer) to render
 * progress by subscribing to logs. Messages are emitted under category "orchestration.progress" via
 * the provided logger.
 *
 * <p>Messages are rate-limited using {@link ProgressRateLimiter} to avoid excessive volume. Each
 * message has a stable shape:
 *
 * <pre>
 * {
 *   "stageId": "exec",
 *   "label": "Executing plan",
 *   "completed": 3,
 *   "total": 7,
 *   "percent": 43,
 *   "message": "operation acme.create finished",
 *   "attrs": { ... },
 *   "status": "running|ok|error|cancelled",
 *   "protocolVersion": 1
 * }
 * </pre>
 */
public class LoggingProgressSink implements ProgressSink {
  private static final int PROTOCOL_VERSION = 1;

  private final org.slf4j.Logger log;
  private final ProgressRateLimiter limiter;

  private final Map<String, Long> totals = new HashMap<>();
  private final Map<String, Long> completions = new HashMap<>();
  private final Map<String, String> labels = new HashMap<>();

  public LoggingProgressSink(org.slf4j.Logger logger, long minIntervalMs, long minDelta) {
    this.log = Objects.requireNonNull(logger, "logger");
    this.limiter = new ProgressRateLimiter(minIntervalMs, minDelta);
  }

  @Override
  public void beginStage(String id, String label, long totalWork) {
    totals.put(id, Math.max(0, totalWork));
    completions.put(id, 0L);
    labels.put(id, label);
    emit(id, label, 0L, totalWork, "begin", Map.of(), "running");
  }

  @Override
  public void step(String id, long completed, String message, Map<String, Object> attrs) {
    long now = System.currentTimeMillis();
    if (limiter.tryAcquire(now, completed)) {
      completions.put(id, completed);
      emit(
          id,
          labels.getOrDefault(id, id),
          completed,
          totals.getOrDefault(id, 0L),
          message,
          attrs == null ? Map.of() : attrs,
          "running");
    }
  }

  @Override
  public void endStageOk(String id, Map<String, Object> attrs) {
    long total = totals.getOrDefault(id, completions.getOrDefault(id, 0L));
    long done = Math.max(total, completions.getOrDefault(id, total));
    emit(
        id,
        labels.getOrDefault(id, id),
        done,
        total,
        "end",
        attrs == null ? Map.of() : attrs,
        "ok");
  }

  @Override
  public void endStageError(String id, String errorSummary, Map<String, Object> attrs) {
    Map<String, Object> merged = new HashMap<>();
    if (attrs != null) merged.putAll(attrs);
    if (errorSummary != null) merged.put("error", errorSummary);
    long total = totals.getOrDefault(id, completions.getOrDefault(id, 0L));
    long done = completions.getOrDefault(id, 0L);
    emit(id, labels.getOrDefault(id, id), done, total, "error", merged, "error");
  }

  /** Build the payload map. Marked protected to facilitate unit testing via subclassing. */
  protected Map<String, Object> createPayload(
      String id,
      String label,
      long completed,
      long total,
      String message,
      Map<String, Object> attrs,
      String status) {
    long safeTotal = Math.max(0, total);
    long safeCompleted = Math.max(0, Math.min(completed, safeTotal == 0 ? completed : safeTotal));
    int percent =
        safeTotal > 0 ? (int) Math.min(100, Math.round((safeCompleted * 100.0) / safeTotal)) : 0;
    return Map.of(
        "stageId", id,
        "label", label,
        "completed", safeCompleted,
        "total", safeTotal,
        "percent", percent,
        "message", message,
        "attrs", attrs == null ? Map.of() : attrs,
        "status", status,
        "protocolVersion", PROTOCOL_VERSION);
  }

  /** Prepare and emit a JSON payload. Extracted for testability. */
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
    log.info("[orchestration.progress] {}", JacksonUtility.toJson(payload));
  }
}
