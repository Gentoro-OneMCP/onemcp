package com.gentoro.onemcp.handbook.model.guardrails;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GuardrailOperations {

  private List<String> supported;
  private List<String> restricted;

  public GuardrailOperations() {
    this.supported = new ArrayList<>();
    this.restricted = new ArrayList<>();
  }

  public List<String> getSupported() {
    return supported;
  }

  public void setSupported(List<String> supported) {
    this.supported =
        new ArrayList<>(Objects.requireNonNullElse(supported, Collections.emptyList()));
    ;
  }

  public void addSupported(String supported) {
    this.supported.add(supported);
  }

  public List<String> getRestricted() {
    return restricted;
  }

  public void setRestricted(List<String> restricted) {
    this.restricted =
        new ArrayList<>(Objects.requireNonNullElse(restricted, Collections.emptyList()));
    ;
  }

  public void addRestricted(String restricted) {
    this.restricted.add(restricted);
  }
}
