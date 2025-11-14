package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.messages.ExecutionPlan;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.Scanner;

public class OrchestratorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OrchestratorService.class);

  private enum Phase {
    PLAN,
    EXECUTE,
    SUMMARY,
    COMPLETED
  }

  private final OneMcp oneMcp;

  public OrchestratorService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  public void enterInteractiveMode() {
    // Create a scanner to read input from the console
    Scanner scanner = new Scanner(System.in);

    try {

      System.out.println("Welcome! Type something (or 'exit' to quit):");

      while (true) {
        System.out.print("> "); // prompt symbol

        // Check if there's input available (avoid blocking on EOF)
        if (!scanner.hasNextLine()) {
          break;
        }

        String input = scanner.nextLine().trim();

        // Exit condition
        if (input.equalsIgnoreCase("exit")) {
          System.out.println("Goodbye!");
          break;
        }

        // Handle normal input
        if (input.trim().length() > 10) {
          try {
            handlePrompt(input, true);
          } catch (Exception e) {
            log.error("Error handling prompt", e);
            StdoutUtility.printError(oneMcp, "Could not handle assignment properly", e);
          }
        }
      }

      // Cleanup
      scanner.close();
    } finally {
      oneMcp.shutdown();
    }
  }

  public String handlePrompt(String prompt) {
    return handlePrompt(prompt, false);
  }

  public String handlePrompt(String prompt, boolean interactive) {
    log.trace("Processing prompt: {}", prompt);

    OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore());
    PlanningService planning = new PlanningService(ctx);
    ImplementationService implementation = new ImplementationService(ctx);
    CodeSnippetService code = new CodeSnippetService(ctx);
    PlanExecutor executor = new PlanExecutor(oneMcp, implementation, code);
    SummaryService summaryService = new SummaryService(ctx);
    Phase currentPhase = Phase.PLAN;
    ExecutionPlan executionPlan = null;
    String summary = null;
    long start = System.currentTimeMillis();
    while (currentPhase != Phase.COMPLETED) {
      switch (currentPhase) {
        case PLAN:
          StdoutUtility.printNewLine(oneMcp, "Generating execution plan.");
          executionPlan = planning.plan(prompt.trim());
          currentPhase = Phase.EXECUTE;
          log.trace(
              "Execution plan:\n{}", StringUtility.formatWithIndent(executionPlan.toString(), 4));
          StdoutUtility.printSuccessLine(
              oneMcp,
              "Executing plan completed in (%d ms).\n%s"
                  .formatted(
                      (System.currentTimeMillis() - start),
                      StringUtility.formatWithIndent(executionPlan.toString(), 4)));
          break;
        case EXECUTE:
          executor.execute(executionPlan);
          currentPhase = Phase.SUMMARY;
          break;
        case SUMMARY:
          StdoutUtility.printNewLine(oneMcp, "Compiling answer.");
          summary = summaryService.summarize(prompt.trim()).answer();
          log.trace("Generated answer:\n{}", StringUtility.formatWithIndent(summary, 4));
          currentPhase = Phase.COMPLETED;
          StdoutUtility.printSuccessLine(
              oneMcp,
              "Assignment handled in (%s ms)\nAnswer: \n%s"
                  .formatted(
                      (System.currentTimeMillis() - start),
                      StringUtility.formatWithIndent(summary, 4)));
          break;
      }
    }

    return summary;
  }
}
