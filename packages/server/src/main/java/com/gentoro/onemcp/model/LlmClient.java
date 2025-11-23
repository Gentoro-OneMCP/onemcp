package com.gentoro.onemcp.model;

import java.util.List;

/**
 * Primary abstraction for interacting with Large Language Model (LLM) providers.
 *
 * <p>Implementations should encapsulate provider-specific SDKs and behaviors (tool/function
 * calling, streaming, token accounting, etc.) and expose a single, simple entry point for agents:
 * {@link #chat(List, List, boolean, InferenceEventListener)}.
 *
 * <p>This interface is implementation-agnostic and intended for use in open source projects.
 * Concrete providers should live behind this interface and be selected via {@link LlmClientFactory}
 * or the {@link java.util.ServiceLoader} managed SPI {@link LlmClientProvider}.
 */
public interface LlmClient {
  /**
   * Process a single-turn interaction with optional tool-calling. Implementations may support
   * tool-calling loops or ignore tools if unsupported.
   *
   * @param tools Optional tools available to the model.
   * @return final model response string.
   */
  String chat(
      List<Message> messages, List<Tool> tools, boolean cacheable, InferenceEventListener listener);

  String generate(
      String message, List<Tool> tools, boolean cacheable, InferenceEventListener listener);

  /**
   * Provider-agnostic telemetry sink that model implementations can use to record tracing
   * information and token usage without depending on orchestrator types.
   */
  interface TelemetrySink {
    void startChild(String name);

    void endCurrentOk(java.util.Map<String, Object> attrs);

    void endCurrentError(java.util.Map<String, Object> attrs);

    void addUsage(Long promptTokens, Long completionTokens, Long totalTokens);

    /** Attributes map of the current span (if any). Implementations may return a live map. */
    java.util.Map<String, Object> currentAttributes();
  }

  /**
   * Binds a telemetry sink to the current thread for the duration of the returned scope.
   * Implementations should read this sink (if present) and record their own spans/attributes.
   */
  interface TelemetryScope extends AutoCloseable {
    @Override
    void close();
  }

  /**
   * Attach a telemetry sink for the duration of a block. Typical usage: try (var ignored =
   * llm.withTelemetry(sink)) { llm.chat(...); }
   */
  TelemetryScope withTelemetry(TelemetrySink sink);

  enum Role {
    SYSTEM,
    ASSISTANT,
    USER
  }

  record Message(Role role, String content) {
    static List<Message> allExcept(List<Message> messages, Role role) {
      return messages.stream().filter(m -> !m.role().equals(role)).toList();
    }

    static boolean contains(List<Message> messages, Role role) {
      return messages.stream().anyMatch(m -> m.role().equals(role));
    }

    static Message findFirst(List<Message> messages, Role role) {
      return messages.stream().filter(m -> m.role().equals(role)).findFirst().orElseThrow();
    }
  }

  enum EventType {
    ON_COMPLETION,
    ON_END,
    ON_TOOL_CALL
  }

  interface InferenceEventListener {
    void on(EventType type, Object data);
  }
}
