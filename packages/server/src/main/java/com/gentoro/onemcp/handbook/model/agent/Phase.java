package com.gentoro.onemcp.handbook.model.agent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** The phases a policy can apply to. */
public enum Phase {
  CONTEXT("context"),
  PLAN("plan");

  private final String value;

  Phase(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @JsonCreator
  public static Phase fromValue(String value) {
    for (Phase p : Phase.values()) {
      if (p.value.equalsIgnoreCase(value)) {
        return p;
      }
    }
    throw new IllegalArgumentException("Unknown Phase: " + value);
  }
}
