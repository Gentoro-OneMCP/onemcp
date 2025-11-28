package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.List;
import java.util.Map;

// No provider SDK imports needed here; telemetry is handled within model implementations.

public class EntityExtractionService {
  private final OrchestratorContext context;

  public EntityExtractionService(OrchestratorContext context) {
    this.context = context;
  }

  public AssignmentContext extractContext(String assignment) {
    int attempts = 0;
    TelemetryTracer.Span parentSpan = context.tracer().startChild("entity_extraction");
    PromptTemplate.PromptSession promptSession =
        context
            .prompts()
            .get("/entity_extraction")
            .newSession()
            .enableOnly(
                "assignment",
                Map.of(
                    "assignment",
                    assignment,
                    "agent",
                    context.handbook().agent(),
                    "error_reported",
                    false));
    while (++attempts <= 3) {
      long start = System.currentTimeMillis();
      String result;
      OrchestratorTelemetrySink sink = new OrchestratorTelemetrySink(context.tracer(), context);
      sink.setPhase("extract");
      try (LlmClient.TelemetryScope ignored = context.llmClient().withTelemetry(sink)) {
        result = context.llmClient().generate(promptSession.renderText(), List.of(), false, null);
        // Store result in telemetry for logging
        if (sink.currentAttributes() != null) {
          sink.currentAttributes().put("response", result);
        }
      }
      long end = System.currentTimeMillis();
      // Annotate the entity_extraction attempt on the parent span
      context.tracer().current().attributes.put("attempt", attempts);
      context.tracer().current().attributes.put("latencyMs", (end - start));
      if (result == null || result.isBlank()) {
        promptSession.enableOnly(
            "assignment",
            Map.of(
                "assignment",
                assignment,
                "error_reported",
                true,
                "result",
                "",
                "error",
                "Did not produce a valid response"));
        continue;
      }

      String jsonContent = StringUtility.extractSnippet(result, "json");
      if (jsonContent == null || jsonContent.isBlank()) {
        promptSession.enableOnly(
            "assignment",
            Map.of(
                "assignment",
                assignment,
                "error_reported",
                true,
                "result",
                result,
                "error",
                "No JSON snippet found in response"));
        continue;
      }

      try {
        AssignmentContext assignmentContext =
            JacksonUtility.getJsonMapper().readValue(jsonContent, AssignmentContext.class);

        // check if entity extraction produced a valid response
        if (assignmentContext.getRefinedAssignment() == null
            || assignmentContext.getRefinedAssignment().isEmpty()) {
          if (assignmentContext.getUnhandledParts() == null
              || assignmentContext.getUnhandledParts().isEmpty()) {
            throw new ExecutionException(
                "The assignment did not produce a valid refined version of the assignment. "
                    + "Either the refined assignment or the unhandled parts must be provided.");
          }
        }

        if (assignmentContext.getRefinedAssignment() != null
            && !assignmentContext.getRefinedAssignment().isEmpty()) {
          if (assignmentContext.getContext() == null || assignmentContext.getContext().isEmpty()) {
            throw new ExecutionException(
                "The extraction did not detect any entities and their corresponding operations.");
          }
          assignmentContext
              .getContext()
              .forEach(
                  c -> {
                    if (c.getOperations() == null || c.getOperations().isEmpty()) {
                      throw new ExecutionException(
                          "Each entity must have at least one operation associated to it, review the entity {%s}."
                              .formatted(c.getEntity()));
                    }
                  });
        }

        // Successfully produced a valid assignment context; close the parent span once and return
        context.tracer().endCurrentOk(Map.of("attempts", attempts, "context", jsonContent));
        return assignmentContext;
      } catch (Exception e) {
        promptSession.enableOnly(
            "assignment",
            Map.of(
                "assignment",
                assignment,
                "error_reported",
                true,
                "result",
                result,
                "error",
                ExceptionUtil.formatCompactStackTrace(e)));
        // Do not end the parent span here; this is a retryable failure. The parent
        // span will be closed either on success (OK) or after all retries (ERROR).
      }
    }

    context.tracer().endCurrentError(Map.of("error", "Failed after retries"));
    throw new ExecutionException("Failed to extract entities from assignment after 3 attempts");
  }

  private static Long toLong(Number n) {
    return n == null ? null : n.longValue();
  }
}
