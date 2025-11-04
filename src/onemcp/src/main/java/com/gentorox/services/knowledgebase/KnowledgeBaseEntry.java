package com.gentorox.services.knowledgebase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Simple record representing a knowledge base entry that an LLM can load on-demand.
 * Each entry has an abstract resource URI (e.g., kb://docs/Agent.md) and a short hint
 * describing what the content contains. Content is the full textual payload.
 *
 * @param resource abstract resource URI, typically starting with kb://
 * @param hint short human-friendly description of the content
 * @param content full textual payload for the resource
 */
public record KnowledgeBaseEntry(
    String resource,
    String hint,
    String content
) {
  @JsonCreator
  public KnowledgeBaseEntry(@JsonProperty("resource") String resource,
                            @JsonProperty("hint") String hint,
                            @JsonProperty("content") String content) {
    this.resource = resource;
    this.hint = hint;
    this.content = content;
  }
}
