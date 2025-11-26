package com.gentoro.onemcp.handbook.model.regression;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestCase {

  @JsonProperty("display-name")
  private String displayName;

  private String prompt;

  @JsonProperty("assert")
  private String assertion;

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getPrompt() {
    return prompt;
  }

  public void setPrompt(String prompt) {
    this.prompt = prompt;
  }

  public String getAssertion() {
    return assertion;
  }

  public void setAssertion(String assertion) {
    this.assertion = assertion;
  }
}
