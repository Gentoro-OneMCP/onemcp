package com.gentorox.services.regression;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a single regression test case loaded from YAML.
 */
public class RegressionTestCase {

    @JsonProperty("display-name")
    private String displayName;

    @JsonProperty("prompt")
    private String prompt;

    // "assert" is a reserved keyword in Java; map it to a different field name
    @JsonProperty("assert")
    private String assertText;

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

    public String getAssertText() {
        return assertText;
    }

    public void setAssertText(String assertText) {
        this.assertText = assertText;
    }
}
