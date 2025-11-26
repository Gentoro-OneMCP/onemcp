package com.gentoro.onemcp.handbook.model.regression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RegressionSuite {
  private String name;
  private String version;
  private List<TestCase> tests;

  public RegressionSuite() {
    this.tests = new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public List<TestCase> getTests() {
    return Collections.unmodifiableList(tests);
  }

  public void setTests(List<TestCase> tests) {
    this.tests = new ArrayList<>(Objects.requireNonNullElse(tests, Collections.emptyList()));
  }

  public void addTest(TestCase testCase) {
    this.tests.add(testCase);
  }
}
