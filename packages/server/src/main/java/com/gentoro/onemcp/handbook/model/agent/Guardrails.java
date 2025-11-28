package com.gentoro.onemcp.handbook.model.agent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

/** Top-level Guardrails section. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Guardrails {

  @JsonProperty("policies")
  private List<Policy> policies;

  public Guardrails() {
    this.policies = new java.util.ArrayList<>();
  }

  public List<Policy> getPolicies() {
    return Collections.unmodifiableList(policies);
  }

  public void setPolicies(List<Policy> policies) {
    this.policies = policies;
  }

  public void addPolicy(Policy policy) {
    policies.add(policy);
  }

  public List<Policy> getContextualPolicies() {
    return getPoliciesByPhase(Phase.CONTEXT);
  }

  public List<Policy> getPoliciesByPhase(Phase phase) {
    return policies.stream()
        .filter(
            policy -> {
              return policy.getPhase().isEmpty() || policy.getPhase().contains(phase);
            })
        .toList();
  }
}
