package com.gentorox.services.regression;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Container mapping for YAML files that define a list of regression tests under the root key "tests".
 */
public class RegressionTestSuite {

    @JsonProperty("tests")
    private List<RegressionTestCase> tests = new ArrayList<>();

    public List<RegressionTestCase> getTests() {
        return tests;
    }

    public void setTests(List<RegressionTestCase> tests) {
        this.tests = tests;
    }
}
