package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.model.LlmClient;
import java.util.Map;
import java.util.Objects;

/**
 * Adapter to allow LLM client implementations to write telemetry directly into TelemetryTracer
 * without depending on orchestrator types at the usage sites.
 */
public class OrchestratorTelemetrySink implements LlmClient.TelemetrySink {
  private final TelemetryTracer tracer;

  public OrchestratorTelemetrySink(TelemetryTracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public void startChild(String name) {
    tracer.startChild(name);
  }

  @Override
  public void endCurrentOk(Map<String, Object> attrs) {
    tracer.endCurrentOk(attrs);
  }

  @Override
  public void endCurrentError(Map<String, Object> attrs) {
    tracer.endCurrentError(attrs);
  }

  @Override
  public void addUsage(Long promptTokens, Long completionTokens, Long totalTokens) {
    currentAttributes().put("prompt.token", Objects.requireNonNullElse(promptTokens, 0L));
    currentAttributes().put("completion.token", Objects.requireNonNullElse(completionTokens, 0L));
    currentAttributes().put("total.token", Objects.requireNonNullElse(totalTokens, 0L));
    tracer.addUsage(promptTokens, completionTokens, totalTokens);
  }

  @Override
  public java.util.Map<String, Object> currentAttributes() {
    TelemetryTracer.Span cur = tracer.current();
    return cur == null ? java.util.Map.of() : cur.attributes;
  }
}
