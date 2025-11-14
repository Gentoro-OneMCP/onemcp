package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.compiler.CompilationResult;
import com.gentoro.onemcp.compiler.ExecutionResult;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.messages.StepImplementation;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.List;
import java.util.Set;

/**
 * CodeSnippetService encapsulates compilation and execution of generated code snippets, including a
 * retry loop for compilation fixes via the LLM when necessary.
 */
public class CodeSnippetService extends AbstractServiceImpl<StepImplementation> {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(CodeSnippetService.class);

  public CodeSnippetService(OrchestratorContext ctx) {
    super(ctx, StepImplementation.class);
  }

  /**
   * Compile the given implementation, asking the LLM to apply fixes when compilation fails. Returns
   * the (potentially updated) implementation. Throws IllegalArgumentException when retries are
   * exhausted.
   */
  public StepImplementation compileWithFixes(StepImplementation impl) {
    long start = System.currentTimeMillis();
    int attempts = 0;
    StepImplementation current = impl;
    while (++attempts < 4) {
      if (context().oneMcp().isInteractiveModeEnabled()) {
        System.out.printf("\r(Compiling): (%d) attempt to compile snippet", attempts);
      }
      log.trace(
          "Compiling snippet (attempt {}):\n{}",
          attempts,
          StringUtility.formatWithIndent(impl.snippet(), 4));
      CompilationResult result =
          context().compiler().compileSnippet(current.qualifiedClassName(), current.snippet());
      if (!result.success()) {
        log.warn(
            "Compilation failed, retrying once more:\n{}",
            StringUtility.formatWithIndent(result.reportedErrors(), 4));
        current =
            super.newSession("/compile")
                .enableOnly(
                    "fix-compilation",
                    "snippet",
                    current.snippet(),
                    "errors",
                    result.reportedErrors())
                .chat();
      } else {
        log.trace(
            "Compilation succeeded in {} attempts and {} ms.",
            attempts,
            System.currentTimeMillis() - start);
        return current;
      }
    }

    throw new ExecutionException("Aborted code compilation after %d attempts.".formatted(attempts));
  }

  /**
   * Execute the compiled snippet and return a concise textual summary. Throws
   * IllegalArgumentException if execution failed or expected variables were not produced.
   */
  public String runAndSummarize(StepImplementation impl) {
    if (context().oneMcp().isInteractiveModeEnabled()) {
      System.out.printf("\r(Executing): Executing %s", impl.qualifiedClassName());
    }
    ExecutionResult exec = context().compiler().runSnippet(impl.qualifiedClassName(), context());

    if (!exec.success()) {
      throw new ExecutionException(exec.reportedErrors());
    }

    Set<String> variables = exec.variables();
    if (variables == null || variables.isEmpty()) {
      throw new ExecutionException(
          "Snippet was reported as successful but no variables were produced.");
    }

    List<String> missingVars =
        variables.stream().filter((varName) -> context().memory().get(varName) == null).toList();
    if (!missingVars.isEmpty()) {
      throw new ExecutionException(
          "Snippet was reported as successful but the following variables were not set: "
              + String.join(", ", missingVars));
    }

    return super.newSession("/execute")
        .enableOnly(
            "execution-summary",
            "variables",
            exec.variables().stream().map(varName -> context().memory().get(varName)).toList())
        .renderText();
  }
}
