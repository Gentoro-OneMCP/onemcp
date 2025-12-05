package com.gentoro.onemcp.indexing.docs;

public final class EntityMatch {
  private String entity;
  private double confidence;
  private String reason;

  public EntityMatch() {}

  public EntityMatch(String entity, double confidence, String reason) {
    this.entity = entity;
    this.confidence = confidence;
    this.reason = reason;
  }

  public String getEntity() {
    return entity;
  }

  public double getConfidence() {
    return confidence;
  }

  public String getReason() {
    return reason;
  }
}
