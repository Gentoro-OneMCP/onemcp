package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.mcp.model.ToolCallContext;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;

/**
 * OrchestratorContext centralizes shared services and state used across the orchestration pipeline
 * (planning, implementation, compilation, execution, and summarization).
 *
 * <p>It wires together the LLM client, prompt templates, knowledge base, in-memory value store, and
 * the Java snippet compiler.
 */
public class OrchestratorContext {
  private final LlmClient llmClient;
  private final PromptRepository promptTemplateManager;
  private final Handbook handbook;
  private final ValueStore valueStore;
  private final OneMcp oneMcp;
  private final TelemetryTracer telemetryTracer;
  private final ToolCallContext toolCallContext;

  public OrchestratorContext(
      OneMcp oneMcp, ValueStore valueStore, ToolCallContext toolCallContext) {
    this.oneMcp = oneMcp;
    this.llmClient = oneMcp().llmClient();
    this.promptTemplateManager = oneMcp().promptRepository();
    this.handbook = oneMcp.handbook();
    this.valueStore = valueStore;
    this.telemetryTracer = new TelemetryTracer();
    this.toolCallContext = toolCallContext;
  }

  /** LLM client used for chatting with the provider. */
  public LlmClient llmClient() {
    return llmClient;
  }

  /** Access to prompt templates. */
  public PromptRepository prompts() {
    return promptTemplateManager;
  }

  /** Knowledge base/documentation access. */
  public Handbook handbook() {
    return handbook;
  }

  /** Shared, persistent variable store across steps/executions. */
  public ValueStore memory() {
    return valueStore;
  }

  public OneMcp oneMcp() {
    return oneMcp;
  }

  /** Tracer used to record nested spans and aggregate token usage. */
  public TelemetryTracer tracer() {
    return telemetryTracer;
  }

  public ToolCallContext toolCallContext() {
    return toolCallContext;
  }
}
