package com.gentorox.services.regression;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregated report for a full regression execution.
 */
public class RegressionReport {
    public static class Item {
        public final String displayName;
        public final boolean passed;
        public final String reason;
        public final String prompt;
        public final String output;

        public Item(String displayName, boolean passed, String reason, String prompt, String output) {
            this.displayName = displayName;
            this.passed = passed;
            this.reason = reason;
            this.prompt = prompt;
            this.output = output;
        }
    }

    private final List<Item> items = new ArrayList<>();

    public void add(Item item) {
        items.add(item);
    }

    public List<Item> items() {
        return items;
    }

    public long passedCount() {
        return items.stream().filter(i -> i.passed).count();
    }

    public long failedCount() {
        return items.stream().filter(i -> !i.passed).count();
    }

    public int total() {
        return items.size();
    }
}
