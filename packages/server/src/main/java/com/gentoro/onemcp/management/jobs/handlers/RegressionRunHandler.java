package com.gentoro.onemcp.management.jobs.handlers;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.ValidationException;
import com.gentoro.onemcp.handbook.model.regression.RegressionSuite;
import com.gentoro.onemcp.handbook.model.regression.TestCase;
import com.gentoro.onemcp.management.jobs.JobContext;
import com.gentoro.onemcp.management.jobs.JobExecution;
import com.gentoro.onemcp.management.jobs.JobHandler;
import com.gentoro.onemcp.management.model.RegressionRequest;
import com.gentoro.onemcp.management.model.RegressionResponse;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.orchestrator.progress.NoOpProgressSink;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.CollectionUtility;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;

/**
 * Executes regression suites referenced by relative paths within the handbook. The execution logic
 * is currently a structural dry-run that validates suites and emits a JSON summary. Future
 * iterations can replace the evaluation with actual LLM-based checks.
 */
public final class RegressionRunHandler implements JobHandler {
  private static final Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(RegressionRunHandler.class);

  private final OneMcp oneMcp;

  public RegressionRunHandler(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  @Override
  public JobExecution execute(
      JobContext ctx,
      byte[] requestBytes,
      String requestContentType,
      ProgressReporter progressReporter)
      throws Exception {
    if (ctx.isCancelled()) throw new InterruptedException("Cancelled before start");

    RegressionRequest regressionRequest =
        JacksonUtility.getJsonMapper().readValue(requestBytes, RegressionRequest.class);
    if (regressionRequest == null) {
      throw new ValidationException("Invalid content, missing one or more required fields");
    }

    progressReporter.reportProgress(-1, -1, "Preparing test cases");
    List<RegressionSuite> entries = new ArrayList<>();
    if (regressionRequest.getPaths().isEmpty()) {
      entries.addAll(oneMcp.handbook().regressionSuites().values());
    } else {
      entries.addAll(
          regressionRequest.getPaths().stream()
              .map(oneMcp.handbook()::optionalRegressionSuite)
              .filter(Optional::isPresent)
              .map(Optional::get)
              .toList());
    }

    List<TestCase> allCases =
        new ArrayList<>(entries.stream().flatMap(s -> s.getTests().stream()).toList());

    long start = System.currentTimeMillis();
    List<RegressionResponse.TestCase> cases = new ArrayList<>();
    for (int i = 0; i < allCases.size(); i++) {
      if (ctx.isCancelled()) throw new InterruptedException("Cancelled during execution");
      TestCase testCase = allCases.get(i);
      progressReporter.reportProgress(
          i, allCases.size(), "Running test: " + testCase.getDisplayName());
      cases.add(runTest(regressionRequest, allCases.get(i)));
    }

    RegressionResponse regressionResponse = new RegressionResponse();
    regressionResponse.setFailed((int) cases.stream().filter(tc -> !tc.isPassed()).count());
    regressionResponse.setPassed((int) cases.stream().filter(tc -> tc.isPassed()).count());
    regressionResponse.setDurationMs(Math.max(1, System.currentTimeMillis() - start));
    regressionResponse.setTestCases(cases);
    byte[] bytes =
        JacksonUtility.getJsonMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsBytes(regressionResponse);
    return new JobExecution(
        bytes, "application/json", "Regression run: " + allCases.size() + " cases");
  }

  private RegressionResponse.TestCase runTest(
      RegressionRequest regressionRequest, TestCase testCase) {
    RegressionResponse.TestCase jsonResult = new RegressionResponse.TestCase();
    jsonResult.setName(testCase.getDisplayName());
    log.trace("Running test: {}", testCase.getDisplayName());
    long start = System.currentTimeMillis();
    try {
      AssigmentResult result =
          oneMcp
              .orchestrator()
              .handlePrompt(
                  testCase.getPrompt(), regressionRequest.getContext(), new NoOpProgressSink());
      log.trace("Test result: {}", result);
      if (result.parts().stream().anyMatch(part -> part.isSupported() && !part.isError())) {
        assertTestCaseResult(jsonResult, result);
      } else {
        jsonResult.setPassed(false);
        jsonResult.setDetails(JacksonUtility.toJson(result));
      }
    } catch (Exception e) {
      jsonResult.setPassed(false);
      jsonResult.setDetails(ExceptionUtil.formatCompactStackTrace(e));
      log.error("Failed to run test: {}", testCase.getDisplayName(), e);
    } finally {
      jsonResult.setDurationMs(System.currentTimeMillis() - start);
      log.trace(
          "Test {} took {} ms", testCase.getDisplayName(), System.currentTimeMillis() - start);
    }
    return jsonResult;
  }

  private void assertTestCaseResult(RegressionResponse.TestCase testCase, AssigmentResult result) {
    log.trace("Asserting result: {}", result);
    final String SECTION_ID = "assert-result";
    final Map<String, Object> initialContext =
        Map.of(
            "testCase",
            testCase,
            "result",
            result.parts().stream()
                .filter(part -> part.isSupported() && !part.isError())
                .map(AssigmentResult.Assignment::content)
                .findFirst()
                .orElseThrow(() -> new ValidationException("No supported part found in result")),
            "error_reported",
            false);

    PromptTemplate.PromptSession promptSession =
        oneMcp
            .promptRepository()
            .get("/assert-test-case")
            .newSession()
            .enableOnly(SECTION_ID, initialContext);

    int attempts = 0;
    while (++attempts <= 3) {
      final String llmResponse =
          oneMcp.llmClient().generate(promptSession.renderText(), List.of(), false, null);
      Function<String, Map<String, Object>> errContextProvider =
          (errorDetails) -> {
            return CollectionUtility.mergeMaps(
                initialContext,
                CollectionUtility.mapOf(
                    String.class,
                    Object.class,
                    "error_reported",
                    true,
                    "result",
                    Objects.requireNonNullElse(llmResponse, ""),
                    "error",
                    errorDetails));
          };

      if (llmResponse == null || llmResponse.isBlank()) {
        promptSession.enableOnly(
            SECTION_ID, errContextProvider.apply("Did not produce a valid response"));
        continue;
      }

      String jsonContent = StringUtility.extractSnippet(llmResponse, "json");
      if (jsonContent == null || jsonContent.isBlank()) {
        promptSession.enableOnly(
            SECTION_ID, errContextProvider.apply("Did not produce a valid response"));
        continue;
      }

      try {
        RegressionResponse.AssertionResult assertionResult =
            JacksonUtility.getJsonMapper()
                .readValue(jsonContent, RegressionResponse.AssertionResult.class);
        testCase.setPassed(assertionResult.isPassed());
        testCase.setDetails(assertionResult.getReason());
      } catch (Exception e) {
        promptSession.enableOnly(
            SECTION_ID, errContextProvider.apply(ExceptionUtil.formatCompactStackTrace(e)));
      }
    }
    throw new ExecutionException("Failed to assert test case output after 3 attempts");
  }
}
