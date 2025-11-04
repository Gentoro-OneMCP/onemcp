package com.gentorox.services.regression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.agent.Orchestrator;
import com.gentorox.services.inference.InferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RegressionService discovers YAML test files, executes prompts via Orchestrator, and evaluates
 * pass/fail using the InferenceService as an LLM judge. It logs per-test outcomes and returns a report.
 */
public class RegressionService {

    private static final Logger LOG = LoggerFactory.getLogger(RegressionService.class);

    private final Path rootFoundationPath;
    private final Orchestrator orchestrator;
    private final InferenceService inferenceService;
    private final ObjectMapper yaml;

    public RegressionService(Path rootFoundationPath, Orchestrator orchestrator, InferenceService inferenceService) {
        this.rootFoundationPath = Objects.requireNonNull(rootFoundationPath, "rootFoundationPath");
        this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator");
        this.inferenceService = Objects.requireNonNull(inferenceService, "inferenceService");
        this.yaml = new ObjectMapper(new YAMLFactory());
    }

    /**
     * Runs all regression tests found under foundation/regression.
     */
    public RegressionReport runAll() {
        Path root = rootFoundationPath.resolve("regression");
        List<Path> files = discoverYamlFiles(root);

        if (files.isEmpty()) {
            LOG.warn("No regression files found under {}", root.toAbsolutePath());
        } else {
            LOG.info("Discovered {} regression file(s) under {}:", files.size(), root.toAbsolutePath());
            files.forEach(p -> LOG.info(" - {}", root.relativize(p)));
        }

        RegressionReport report = new RegressionReport();
        for (Path file : files) {
            RegressionTestSuite suite = readSuite(file);
            if (suite == null || suite.getTests() == null || suite.getTests().isEmpty()) {
                LOG.warn("No tests in YAML file: {}", file);
                continue;
            }
            LOG.info("Executing {} test(s) from {}", suite.getTests().size(), file.getFileName());
            for (RegressionTestCase test : suite.getTests()) {
                executeTest(test, report);
            }
        }

        long passed = report.passedCount();
        long failed = report.failedCount();
        int total = report.total();
        LOG.info("\n================ Regression Summary ================");
        LOG.info("Total: {} | Passed: {} | Failed: {}", total, passed, failed);
        if (failed > 0) {
            LOG.info("Failed tests:");
            for (RegressionReport.Item it : report.items().stream().filter(i -> !i.passed).toList()) {
                LOG.info(" - {} :: {}", it.displayName, it.reason);
            }
        }
        LOG.info("===================================================\n");

        return report;
    }

    private void executeTest(RegressionTestCase test, RegressionReport report) {
        String name = Optional.ofNullable(test.getDisplayName()).orElse("<unnamed>");
        String prompt = Optional.ofNullable(test.getPrompt()).orElse("");
        String criteria = Optional.ofNullable(test.getAssertText()).orElse("");

        try {
            // 1) Run the user prompt through the Orchestrator
            InferenceResponse resp = orchestrator.run(
                    List.of(new InferenceRequest.Message("user", prompt)),
                    Map.of()
            );
            String output = Optional.ofNullable(resp).map(InferenceResponse::content).orElse("");

            // 2) Ask the model (via InferenceService) to judge the output against the criteria
            Verdict verdict = judge(name, criteria, output);

            // 3) Log and record
            if (verdict.pass) {
                LOG.info("[PASS] {}", name);
                LOG.warn("[PASS] Produced value: {}", output);
            } else {
                LOG.warn("[FAIL] {} :: {}", name, verdict.reason);
                LOG.warn("[FAIL] Produced value: {}", output);
            }
            report.add(new RegressionReport.Item(name, verdict.pass, verdict.reason, prompt, output));
        } catch (Exception e) {
            String msg = e.getMessage() == null ? e.toString() : e.getMessage();
            LOG.error("[ERROR] {} :: {}", name, msg);
            report.add(new RegressionReport.Item(name, false, "Execution error: " + msg, prompt, ""));
        }
    }

    private Verdict judge(String displayName, String criteria, String output) {
        String evalPrompt = buildJudgePrompt(displayName, criteria, output);
        var resp = inferenceService.sendRequest(evalPrompt);
        String content = Optional.ofNullable(resp).map(InferenceResponse::content).orElse("");
        Verdict v = parseVerdict(content);
        if (v == null) {
            // Fallback heuristic: consider pass if content starts with PASS
            boolean pass = content.trim().toUpperCase(Locale.ROOT).startsWith("PASS");
            String reason = pass ? "Heuristic PASS (response begins with PASS)" : truncate(content, 400);
            return new Verdict(pass, reason);
        }
        return v;
    }

    private String buildJudgePrompt(String displayName, String criteria, String output) {
        return "You are a strict test evaluator.\n" +
                "Given the test details and the model output, decide if the test PASSES.\n" +
                "Respond ONLY in JSON with the following schema on a single line: " +
                "{\"pass\": boolean, \"reason\": string}.\n" +
                "Reason must be concise.\n\n" +
                "Test: " + displayName + "\n" +
                "Criteria:\n" + criteria + "\n\n" +
                "ModelOutput:\n" + output + "\n\n" +
                "Return JSON now.";
    }

    private Verdict parseVerdict(String content) {
        if (content == null || content.isBlank()) return null;
        try {
            Map<?,?> m = new ObjectMapper().readValue(content, Map.class);
            Object p = m.get("pass");
            Object r = m.get("reason");
            if (p instanceof Boolean b) {
                return new Verdict(b, r == null ? "" : String.valueOf(r));
            }
        } catch (Exception ignored) {
            // Fall through to pattern-based parsing
        }
        // Try simple key detection
        String upper = content.toUpperCase(Locale.ROOT);
        boolean pass = upper.contains("\"PASS\":TRUE") || upper.matches(".*\\bPASS\\b.*");
        String reason = extractReason(content);
        return new Verdict(pass, reason);
    }

    private String extractReason(String content) {
        // Try to find a line with "reason" or after VERDICT
        for (String line : content.split("\n")) {
            String l = line.trim();
            if (l.toLowerCase(Locale.ROOT).startsWith("reason")) {
                int idx = l.indexOf(':');
                return idx > 0 ? l.substring(idx + 1).trim() : l;
            }
        }
        return truncate(content, 400);
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private RegressionTestSuite readSuite(Path file) {
        try {
            return yaml.readValue(Files.newBufferedReader(file), RegressionTestSuite.class);
        } catch (IOException e) {
            LOG.error("Failed to parse YAML file {}: {}", file, e.getMessage());
            return null;
        }
    }

    private List<Path> discoverYamlFiles(Path root) {
        if (!Files.exists(root)) return List.of();
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String n = p.getFileName().toString().toLowerCase(Locale.ROOT);
                        return n.endsWith(".yaml") || n.endsWith(".yml");
                    })
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.error("Failed to scan regression directory {}: {}", root, e.getMessage());
            return List.of();
        }
    }

    private record Verdict(boolean pass, String reason) {}
}
