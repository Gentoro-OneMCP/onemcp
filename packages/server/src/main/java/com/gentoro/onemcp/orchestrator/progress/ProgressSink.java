package com.gentoro.onemcp.orchestrator.progress;

import java.util.Map;

/**
 * Progress reporting abstraction for long-running orchestration flows.
 *
 * <p>This decouples the {@code OrchestratorService} (producer of progress events) from the
 * transport layer (MCP notifications, logs, etc.). Implementations are expected to be lightweight
 * and non-blocking. They should also apply their own rate limiting to avoid spamming downstream
 * consumers.
 */
public interface ProgressSink {

  /**
   * Signal the beginning of a stage.
   *
   * @param id stable stage identifier (e.g., "extract", "plan", "prepare", "exec", "finalize")
   * @param label human-readable label for presentation
   * @param totalWork total work units (may be 0 or unknown). For execution, this can be the total
   *     number of operations. Implementations should handle 0/unknown gracefully.
   */
  void beginStage(String id, String label, long totalWork);

  /**
   * Report an incremental step within a stage.
   *
   * @param id stage identifier
   * @param completed completed work units so far (monotonic, between 0..totalWork)
   * @param message short message describing the current step
   * @param attrs optional structured attributes (e.g., service, operation, latencyMs)
   */
  void step(String id, long completed, String message, Map<String, Object> attrs);

  /** Mark a stage as successfully completed. */
  void endStageOk(String id, Map<String, Object> attrs);

  /** Mark a stage as failed with a short error summary (no PII). */
  void endStageError(String id, String errorSummary, Map<String, Object> attrs);

  /**
   * Return true if the current operation has been cancelled by the caller. Implementations that do
   * not support cancellation should always return false.
   */
  default boolean isCancelled() {
    return false;
  }
}
