package com.gentoro.onemcp.handbook.model.guardrails;

import com.gentoro.onemcp.handbook.model.agent.Release;
import java.util.List;

public class Guardrails {
  private List<Release> releases;
  private String additionalInstructions;
  private List<String> boundaries;
  private GuardrailOperations operations;

  public Guardrails() {
    this.releases = new java.util.ArrayList<>(List.of(new Release()));
    this.boundaries = new java.util.ArrayList<>();
  }

  public List<Release> getReleases() {
    return releases;
  }

  public void setReleases(List<Release> releases) {
    this.releases = releases;
  }

  public String getAdditionalInstructions() {
    return additionalInstructions;
  }

  public void setAdditionalInstructions(String additionalInstructions) {
    this.additionalInstructions = additionalInstructions;
  }

  public List<String> getBoundaries() {
    return boundaries;
  }

  public void setBoundaries(List<String> boundaries) {
    this.boundaries = boundaries;
  }

  public GuardrailOperations getOperations() {
    return operations;
  }

  public void setOperations(GuardrailOperations operations) {
    this.operations = operations;
  }
}
