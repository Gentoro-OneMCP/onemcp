package com.gentorox.services.agent;

import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;

import java.util.List;
import java.util.Map;

/**
 * Orchestrator coordinates incoming client intents with internal services:
 * - Builds a personalized system prompt using AgentService and the Knowledge Base
 * - Enumerates available tools and services derived from OpenAPI/TypeScript runtime
 * - Verifies the request against configured guardrails
 * - Delegates inference to the active model provider
 * - Emits rich telemetry for tracing and metrics
 *
 * Implementations MUST be thread-safe.
 */
public interface Orchestrator {

  /**
   * Execute an inference flow based on client-provided messages.
   * The first user message typically carries the "prompt" intent, while additional
   * messages provide short-term context.
   *
   * @param messages ordered conversation messages
   * @param options  optional opaque options (headers, auth, telemetry ids, etc.)
   * @return model response containing content and optional tool call
   */
  InferenceResponse run(List<InferenceRequest.Message> messages, Map<String, Object> options);
}
