package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Prompt Schema Workflow (PSW) - A sequence of Prompt Schemas.
 *
 * <p>Represents prompts that decompose into multiple sequential steps.
 * Example: "Find flights to NYC, pick the cheapest one, and book it."
 *
 * <p>Currently supports "sequential" workflow type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptSchemaWorkflow {
  @JsonProperty("workflow_type")
  private String workflowType = "sequential";

  @JsonProperty("steps")
  private List<PromptSchemaStep> steps = new ArrayList<>();

  public PromptSchemaWorkflow() {}

  public PromptSchemaWorkflow(String workflowType, List<PromptSchemaStep> steps) {
    this.workflowType = workflowType != null ? workflowType : "sequential";
    this.steps = steps != null ? new ArrayList<>(steps) : new ArrayList<>();
  }

  public String getWorkflowType() {
    return workflowType;
  }

  public void setWorkflowType(String workflowType) {
    this.workflowType = workflowType != null ? workflowType : "sequential";
  }

  public List<PromptSchemaStep> getSteps() {
    return steps;
  }

  public void setSteps(List<PromptSchemaStep> steps) {
    this.steps = steps != null ? new ArrayList<>(steps) : new ArrayList<>();
  }

  /**
   * Validate this workflow against a dictionary.
   *
   * @param dictionary the dictionary to validate against
   * @return list of validation errors (empty if valid)
   */
  public List<String> validate(PromptDictionary dictionary) {
    List<String> errors = new ArrayList<>();

    if (workflowType == null || workflowType.isEmpty()) {
      errors.add("Workflow type is required");
    } else if (!"sequential".equals(workflowType)) {
      errors.add("Unsupported workflow type: " + workflowType);
    }

    if (steps == null || steps.isEmpty()) {
      errors.add("At least one step is required");
    } else {
      for (int i = 0; i < steps.size(); i++) {
        PromptSchemaStep step = steps.get(i);
        if (step == null || step.getPs() == null) {
          errors.add("Step " + (i + 1) + " is null or missing PromptSchema");
        } else {
          List<String> stepErrors = step.getPs().validate(dictionary);
          for (String error : stepErrors) {
            errors.add("Step " + (i + 1) + ": " + error);
          }
        }
      }
    }

    return errors;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PromptSchemaWorkflow that = (PromptSchemaWorkflow) o;
    return Objects.equals(workflowType, that.workflowType) && Objects.equals(steps, that.steps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workflowType, steps);
  }

  @Override
  public String toString() {
    return "PromptSchemaWorkflow{"
        + "workflowType='"
        + workflowType
        + '\''
        + ", steps="
        + steps
        + '}';
  }
}



