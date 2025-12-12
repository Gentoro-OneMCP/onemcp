package com.gentoro.onemcp.plan;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.orchestrator.TelemetryTracer;
import com.gentoro.onemcp.prompt.PromptRepository;

/**
 * Context for plan generation and execution in the caching path.
 * Similar to OrchestratorContext but focused on plan-related operations.
 */
public class PlanContext {
  private final LlmClient llmClient;
  private final PromptRepository promptRepository;
  private final Handbook handbook;
  private final ValueStore valueStore;
  private final OneMcp oneMcp;
  private final TelemetryTracer telemetryTracer;

  public PlanContext(OneMcp oneMcp, ValueStore valueStore) {
    this.oneMcp = oneMcp;
    this.llmClient = oneMcp.llmClient();
    this.promptRepository = oneMcp.promptRepository();
    this.handbook = oneMcp.handbook();
    this.valueStore = valueStore;
    this.telemetryTracer = new TelemetryTracer();
  }

  public LlmClient llmClient() {
    return llmClient;
  }

  public PromptRepository prompts() {
    return promptRepository;
  }

  public Handbook handbook() {
    return handbook;
  }

  public ValueStore memory() {
    return valueStore;
  }

  public OneMcp oneMcp() {
    return oneMcp;
  }

  public TelemetryTracer tracer() {
    return telemetryTracer;
  }
}












