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
  private final OrchestratorContext context;
  private String currentPhase = "unknown";
  private java.util.List<LlmClient.Message> currentMessages = null;
  private long currentStartTime = 0;

  public OrchestratorTelemetrySink(TelemetryTracer tracer) {
    this.tracer = tracer;
    this.context = null;
  }

  public OrchestratorTelemetrySink(TelemetryTracer tracer, OrchestratorContext context) {
    this.tracer = tracer;
    this.context = context;
  }

  public void setPhase(String phase) {
    this.currentPhase = phase != null ? phase : "unknown";
    // Store phase in attributes immediately so LLM clients can access it
    if (currentAttributes() != null) {
      currentAttributes().put("phase", this.currentPhase);
    }
  }

  @Override
  public void startChild(String name) {
    tracer.startChild(name);
    // Use span name as phase if phase hasn't been explicitly set
    // This preserves explicit setPhase() calls while allowing span names to be used as fallback
    if (name != null && "unknown".equals(currentPhase)) {
      // Only use span name if it looks like a phase (not a provider name like "llm.anthropic")
      if (!name.startsWith("llm.") && !name.startsWith("operation:")) {
        currentPhase = name;
      }
    }
    // Ensure phase is always available in the current span's attributes
    // This is critical so that detectPhase() can find it even in child spans
    if (currentAttributes() != null) {
      currentAttributes().put("phase", currentPhase);
    }
    currentStartTime = System.currentTimeMillis();
    
    // Log LLM inference start
    if (context != null && context.oneMcp() != null) {
      context.oneMcp().inferenceLogger().logLlmInferenceStart(currentPhase);
    }
  }

  @Override
  public void endCurrentOk(Map<String, Object> attrs) {
    // Store phase in attributes so AbstractLlmClient can access it
    if (currentAttributes() != null) {
      currentAttributes().put("phase", currentPhase);
    }
    
    tracer.endCurrentOk(attrs);
    
    // Log LLM inference complete with tokens and duration
    // Note: This is a fallback - concrete implementations should log directly
    // But we'll try to capture what we can from telemetry
    if (context != null && context.oneMcp() != null && currentStartTime > 0) {
      long duration = System.currentTimeMillis() - currentStartTime;
      Long promptTokens = (Long) currentAttributes().get("_promptTokens");
      Long completionTokens = (Long) currentAttributes().get("_completionTokens");
      if (promptTokens == null) {
        Object pt = currentAttributes().get("prompt.token");
        if (pt instanceof Number) promptTokens = ((Number) pt).longValue();
        else promptTokens = 0L;
      }
      if (completionTokens == null) {
        Object ct = currentAttributes().get("completion.token");
        if (ct instanceof Number) completionTokens = ((Number) ct).longValue();
        else completionTokens = 0L;
      }
      
      // Get response from attributes if available
      String response = attrs != null ? (String) attrs.get("response") : null;
      if (response == null) {
        response = (String) currentAttributes().get("response");
      }
      
      // Only log if we haven't already logged (concrete implementations should handle this)
      // But we'll log anyway as a fallback since generate() might not log properly
      if (response != null || promptTokens > 0 || completionTokens > 0) {
        context.oneMcp().inferenceLogger().logLlmInferenceComplete(
            currentPhase, duration, promptTokens, completionTokens, response != null ? response : "");
      }
    }
    currentStartTime = 0;
  }

  @Override
  public void endCurrentError(Map<String, Object> attrs) {
    tracer.endCurrentError(attrs);
    
    // Log LLM inference complete even on error
    if (context != null && context.oneMcp() != null && currentStartTime > 0) {
      long duration = System.currentTimeMillis() - currentStartTime;
      Long promptTokens = (Long) currentAttributes().get("_promptTokens");
      Long completionTokens = (Long) currentAttributes().get("_completionTokens");
      if (promptTokens == null) promptTokens = 0L;
      if (completionTokens == null) completionTokens = 0L;
      
      String errorMsg = attrs != null ? (String) attrs.get("error") : "Error occurred";
      context.oneMcp().inferenceLogger().logLlmInferenceComplete(
          currentPhase, duration, promptTokens, completionTokens, "ERROR: " + errorMsg);
    }
    currentStartTime = 0;
  }
  
  public void setCurrentMessages(java.util.List<LlmClient.Message> messages) {
    this.currentMessages = messages;
    if (context != null && context.oneMcp() != null && messages != null) {
      context.oneMcp().inferenceLogger().logLlmInputMessages(messages);
    }
  }

  @Override
  public void addUsage(Long promptTokens, Long completionTokens, Long totalTokens) {
    currentAttributes().put("prompt.token", Objects.requireNonNullElse(promptTokens, 0L));
    currentAttributes().put("completion.token", Objects.requireNonNullElse(completionTokens, 0L));
    currentAttributes().put("total.token", Objects.requireNonNullElse(totalTokens, 0L));
    tracer.addUsage(promptTokens, completionTokens, totalTokens);
    
    // Store token usage for logging
    currentAttributes().put("_promptTokens", promptTokens);
    currentAttributes().put("_completionTokens", completionTokens);
  }

  @Override
  public java.util.Map<String, Object> currentAttributes() {
    TelemetryTracer.Span cur = tracer.current();
    return cur == null ? java.util.Map.of() : cur.attributes;
  }
}
