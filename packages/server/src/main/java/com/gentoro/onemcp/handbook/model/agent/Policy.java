package com.gentoro.onemcp.handbook.model.agent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

/** Represents a single guardrail policy entry. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Policy {

  /** Unique policy ID */
  @JsonProperty("id")
  private String id;

  /** Optional list of phases where the policy applies */
  @JsonProperty("phase")
  private List<Phase> phase;

  /** Description of the policy */
  @JsonProperty("description")
  private String description;

  public Policy() {
    this.phase = new java.util.ArrayList<>();
  }

  // Getters & Setters

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<Phase> getPhase() {
    return Collections.unmodifiableList(phase);
  }

  public void setPhase(List<Phase> phase) {
    this.phase = phase;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
