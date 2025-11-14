package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.messages.ExecutionPlan;
import com.gentoro.onemcp.messages.StepImplementation;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.HashMap;
import java.util.Map;

/**
 * PlanExecutor executes an {@link ExecutionPlan} by iterating steps and, for each, designing an
 * implementation, compiling with fixes, and running it. It applies retry logic across
 * design/compile/run attempts similarly to the legacy flow.
 */
public class PlanExecutor {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(PlanExecutor.class);
  private final ImplementationService implementationService;
  private final CodeSnippetService codeService;
  private final OneMcp oneMcp;

  public PlanExecutor(
      OneMcp oneMcp, ImplementationService implementationService, CodeSnippetService codeService) {
    this.oneMcp = oneMcp;
    this.implementationService = implementationService;
    this.codeService = codeService;
  }

  /** Execute the plan and return a per-step textual summary report. */
  public Map<String, String> execute(ExecutionPlan plan) {
    Map<String, String> summary = new HashMap<>();
    int currentStep = 0;

    while (currentStep < plan.steps().size()) {
      ExecutionPlan.Step step = plan.steps().get(currentStep);
      StdoutUtility.printNewLine(oneMcp, "(Executing): executing step %s".formatted(step.title()));
      long start = System.currentTimeMillis();
      try {
        summary.put(step.title(), executeStep(step));
        StdoutUtility.printSuccessLine(
            oneMcp,
            "(Executing): completed in (%s ms):\n  Title:\n%s\n  Summary:\n%s\n"
                .formatted(
                    System.currentTimeMillis() - start,
                    StringUtility.formatWithIndent(step.title(), 4),
                    StringUtility.formatWithIndent(summary.get(step.title()), 4, 5)));

      } catch (Exception e) {
        StdoutUtility.printError(oneMcp, "(Executing): Failure:", e);
        log.error("Error executing step {}", step.title(), e);
        summary.put(step.title(), e.getMessage());
        break;
      }
      currentStep++;
    }

    return summary;
  }

  private String executeStep(ExecutionPlan.Step step) {
    String[] attempt = null;
    int attempts = 0;

    while (true) {
      if (++attempts == 3) {
        throw new ExecutionException("There are more than 3 attempts to execute step");
      }

      StepImplementation impl = null;
      try {
        // Ask LLM to design the code for this step
        impl = implementationService.designStep(step, attempt);
        log.info(
            "Generated execution resources for step:\n{}\n---\n",
            StringUtility.formatWithIndent(impl.toString(), 4));
      } catch (Exception e) {
        log.error("Error executing step {}, retrying.", step.title(), e);
        continue;
      }

      try {
        // Compile; on failure, ask LLM to fix and retry
        impl = codeService.compileWithFixes(impl);
      } catch (Exception e) {
        log.error("Compilation failed, retrying.", e);
        attempt = new String[] {impl.snippet(), ExceptionUtil.formatCompactStackTrace(e)};
        continue;
      }

      try {
        // Execute implementation and return its textual summary
        return codeService.runAndSummarize(impl);
      } catch (Exception e) {
        log.error("Error executing step {}, retrying.", step.title(), e);
        attempt = new String[] {impl.snippet(), ExceptionUtil.formatCompactStackTrace(e)};
      }
    }
  }
}
