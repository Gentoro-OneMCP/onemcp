package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.compiler.JavaSnippetCompiler;
import com.gentoro.onemcp.context.KnowledgeBase;
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
  private final KnowledgeBase knowledgeBase;
  private final ValueStore valueStore;
  private final JavaSnippetCompiler javaSnippetCompiler;
  private final OneMcp oneMcp;

  public OrchestratorContext(OneMcp oneMcp, ValueStore valueStore) {
    this.oneMcp = oneMcp;
    this.llmClient = oneMcp().llmClient();
    this.promptTemplateManager = oneMcp().promptRepository();
    this.knowledgeBase = oneMcp.knowledgeBase();
    this.valueStore = valueStore;
    this.javaSnippetCompiler = new JavaSnippetCompiler();
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
  public KnowledgeBase knowledgeBase() {
    return knowledgeBase;
  }

  /** Shared, persistent variable store across steps/executions. */
  public ValueStore memory() {
    return valueStore;
  }

  /** Compiler for generated Java snippets. */
  public JavaSnippetCompiler compiler() {
    return javaSnippetCompiler;
  }

  public OneMcp oneMcp() {
    return oneMcp;
  }
}
