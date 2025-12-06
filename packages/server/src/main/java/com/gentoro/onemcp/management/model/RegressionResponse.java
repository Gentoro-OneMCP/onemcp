package com.gentoro.onemcp.management.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RegressionResponse {
  private int passed;
  private int failed;
  private long durationMs;
  private List<TestCase> testCases;

  public RegressionResponse() {
    this.testCases = new ArrayList<>();
  }

  public int getPassed() {
    return passed;
  }

  public void setPassed(int passed) {
    this.passed = passed;
  }

  public int getFailed() {
    return failed;
  }

  public void setFailed(int failed) {
    this.failed = failed;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public void setDurationMs(long durationMs) {
    this.durationMs = durationMs;
  }

  public List<TestCase> getTestCases() {
    return Collections.unmodifiableList(testCases);
  }

  public void setTestCases(List<TestCase> testCases) {
    this.testCases.clear();
    this.testCases.addAll(Objects.requireNonNullElse(testCases, Collections.emptyList()));
  }

  public static class TestCase {
    private String name;
    private boolean passed;
    private String details;
    private long durationMs;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public boolean isPassed() {
      return passed;
    }

    public void setPassed(boolean passed) {
      this.passed = passed;
    }

    public String getDetails() {
      return details;
    }

    public void setDetails(String details) {
      this.details = details;
    }

    public long getDurationMs() {
      return durationMs;
    }

    public void setDurationMs(long durationMs) {
      this.durationMs = durationMs;
    }
  }

  public static class AssertionResult {
    private boolean passed;
    private String reason;

    public boolean isPassed() {
      return passed;
    }

    public void setPassed(boolean passed) {
      this.passed = passed;
    }

    public String getReason() {
      return reason;
    }

    public void setReason(String reason) {
      this.reason = reason;
    }
  }
}
