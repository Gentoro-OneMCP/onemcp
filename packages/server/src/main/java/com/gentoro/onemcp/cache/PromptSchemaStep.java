package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * A single step in a Prompt Schema Workflow.
 *
 * <p>Contains a Prompt Schema (PS) representing one step in a multi-step workflow.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptSchemaStep {
  @JsonProperty("ps")
  private PromptSchema ps;

  public PromptSchemaStep() {}

  public PromptSchemaStep(PromptSchema ps) {
    this.ps = ps;
  }

  public PromptSchema getPs() {
    return ps;
  }

  public void setPs(PromptSchema ps) {
    this.ps = ps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PromptSchemaStep that = (PromptSchemaStep) o;
    return Objects.equals(ps, that.ps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ps);
  }

  @Override
  public String toString() {
    return "PromptSchemaStep{" + "ps=" + ps + '}';
  }
}
