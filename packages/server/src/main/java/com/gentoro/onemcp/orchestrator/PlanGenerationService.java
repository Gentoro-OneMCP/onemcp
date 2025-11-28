package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.engine.ExecutionPlanValidator;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.indexing.ContextDecorator;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.Operation;
import java.util.List;
import java.util.Map;

// No provider SDK imports needed here; telemetry is handled within model implementations.

public class PlanGenerationService {
  private final OrchestratorContext context;

  public PlanGenerationService(OrchestratorContext context) {
    this.context = context;
  }

  public JsonNode generatePlan(
      AssignmentContext assignmentContext, List<Map<String, Object>> contextualData) {
    int attempts = 0;
    TelemetryTracer.Span parentSpan = context.tracer().startChild("plan_generation");
    PromptTemplate.PromptSession promptSession =
        context
            .prompts()
            .get("/plan_generation")
            .newSession()
            .enableOnly(
                "assignment",
                Map.of(
                    "assignment",
                    assignmentContext.getRefinedAssignment(),
                    "agent",
                    context.handbook().agent(),
                    "context",
                    new ContextDecorator(contextualData),
                    "error_reported",
                    false));
    while (++attempts <= 3) {
      long start = System.currentTimeMillis();
      String result;
      OrchestratorTelemetrySink sink = new OrchestratorTelemetrySink(context.tracer(), context);
      sink.setPhase("plan");
      try (LlmClient.TelemetryScope ignored = context.llmClient().withTelemetry(sink)) {
        result = context.llmClient().generate(promptSession.renderText(), List.of(), false, null);
        // Store result in telemetry for logging
        if (sink.currentAttributes() != null) {
          sink.currentAttributes().put("response", result);
        }
      }
      long end = System.currentTimeMillis();
      context.tracer().current().attributes.put("attempt", attempts);
      context.tracer().current().attributes.put("latencyMs", (end - start));
      if (result == null || result.isBlank()) {
        promptSession.enableOnly(
            "assignment",
            Map.of(
                "assignment",
                assignmentContext.getRefinedAssignment(),
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
                assignmentContext.getRefinedAssignment(),
                "error_reported",
                true,
                "result",
                result,
                "error",
                "No JSON snippet found in response"));
        continue;
      }

      try {
        List<String> listOfAllowedOperations =
            context.handbook().apis().values().stream()
                .flatMap(a -> a.getService().getOpenApi().getPaths().values().stream())
                .flatMap(p -> p.readOperations().stream())
                .map(Operation::getOperationId)
                .toList();

        JsonNode executionPlan = JacksonUtility.getJsonMapper().readTree(jsonContent);
        ExecutionPlanValidator.validate(executionPlan, listOfAllowedOperations);
        // Success: close the parent span only once and return
        context.tracer().endCurrentOk(Map.of("attempts", attempts, "execution_plan", jsonContent));
        return executionPlan;
      } catch (Exception e) {
        promptSession.enableOnly(
            "assignment",
            Map.of(
                "assignment",
                assignmentContext.getRefinedAssignment(),
                "error_reported",
                true,
                "result",
                result,
                "error",
                ExceptionUtil.formatCompactStackTrace(e)));
        // Do not close the parent span here; it will be closed after retries fail or on success.
      }
    }

    context.tracer().endCurrentError(Map.of("error", "Failed after retries"));
    throw new ExecutionException(
        "Failed to generate execution plan from assignment after 3 attempts");
  }

  private static Long toLong(Number n) {
    return n == null ? null : n.longValue();
  }
}
