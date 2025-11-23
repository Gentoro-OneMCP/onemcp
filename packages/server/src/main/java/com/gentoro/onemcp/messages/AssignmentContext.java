package com.gentoro.onemcp.messages;

import java.util.List;

public class AssignmentContext {
  private String refinedAssignment;
  private String unhandledParts;
  private List<Context> context;

  public AssignmentContext() {
    this.context = new java.util.ArrayList<>();
  }

  public AssignmentContext(List<Context> context) {
    this.context = context;
  }

  public List<Context> getContext() {
    return context;
  }

  public void setContext(List<Context> context) {
    this.context = context;
  }

  public String getRefinedAssignment() {
    return refinedAssignment;
  }

  public void setRefinedAssignment(String refinedAssignment) {
    this.refinedAssignment = refinedAssignment;
  }

  public String getUnhandledParts() {
    return unhandledParts;
  }

  public void setUnhandledParts(String unhandledParts) {
    this.unhandledParts = unhandledParts;
  }

  public static class Context {
    private String entity;
    private List<String> operations;
    private int confidence;
    private String referral;

    public Context() {}

    public Context(String entity, List<String> operations, int confidence, String referral) {
      this.entity = entity;
      this.operations = operations;
      this.confidence = confidence;
      this.referral = referral;
    }

    public String getEntity() {
      return entity;
    }

    public void setEntity(String entity) {
      this.entity = entity;
    }

    public List<String> getOperations() {
      return operations;
    }

    public void setOperation(List<String> operations) {
      this.operations = operations;
    }

    public int getConfidence() {
      return confidence;
    }

    public void setConfidence(int confidence) {
      this.confidence = confidence;
    }

    public String getReferral() {
      return referral;
    }

    public void setReferral(String referral) {
      this.referral = referral;
    }
  }
}
