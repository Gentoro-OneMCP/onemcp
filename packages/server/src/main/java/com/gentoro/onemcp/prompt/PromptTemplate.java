package com.gentoro.onemcp.prompt;

import com.gentoro.onemcp.model.LlmClient;
import java.util.List;
import java.util.Map;

/**
 * Immutable definition of a prompt template composed of multiple sections. Use {@link
 * PromptSession} to select sections and render.
 */
public interface PromptTemplate {
  /** Identifier of this template (e.g., "ollama/summary"). */
  String id();

  /** Read-only view of the sections defined by this template. */
  List<PromptSection> sections();

  /** Create a new mutable session to enable/disable sections and render. */
  PromptSession newSession();

  /** A single prompt section (message) definition. */
  record PromptSection(LlmClient.Role role, String id, boolean enabledByDefault, String content) {}

  /** Per-render mutable context used to enable/disable sections and render output. */
  interface PromptSession {
    PromptSession enable(String sectionId, Map<String, Object> vars);

    PromptSession enableOnly(String sectionId, Map<String, Object> vars);

    PromptSession disable(String... sectionIds);

    /** Disable all sections. */
    PromptSession clear();

    /** Reset the enabled state to YAML defaults. */
    PromptSession resetToDefaults();

    List<LlmClient.Message> renderMessages();

    String renderText();
  }
}
