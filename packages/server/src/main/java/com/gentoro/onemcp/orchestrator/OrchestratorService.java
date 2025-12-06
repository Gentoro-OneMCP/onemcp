package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.mcp.model.ToolCallContext;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.orchestrator.progress.ProgressSink;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.*;

public class OrchestratorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OrchestratorService.class);

  private final OneMcp oneMcp;

  public OrchestratorService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Handle a natural language prompt request and return a structured result, emitting optional
   * progress updates through the provided {@link ProgressSink}.
   */
  public AssigmentResult handlePrompt(
      String prompt, ToolCallContext toolCallContext, ProgressSink progress) {
    log.trace("Processing prompt: {}", prompt);

    if (oneMcp.handbook().apis().isEmpty()) {
      throw new HandbookException(
          "Cannot handle assignments just yet, there are no APIs loaded. "
              + "Review your handbook configuration and publish your APIs.");
    }

    final List<String> calledOperations = new ArrayList<>();
    final List<AssigmentResult.Assignment> assignmentParts = new ArrayList<>();
    final long start = System.currentTimeMillis();
    AssignmentContext assignmentContext = null;
    OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore(), toolCallContext);
    try {
      // annotate root span with incoming prompt (truncated to avoid sensitive data)
      TelemetryTracer tracer = ctx.tracer();
      if (tracer.current() != null) {
        String promptPreview = prompt.length() > 500 ? prompt.substring(0, 500) + "…" : prompt;
        tracer.current().attributes.put("prompt.preview", promptPreview);
      }

      // Extract entities stage
      progress.beginStage("extract", "Extracting entities", 1);
      assignmentContext = new EntityExtractionService(ctx).extractContext(prompt.trim());
      int entityCount =
          assignmentContext.getContext() == null ? 0 : assignmentContext.getContext().size();
      progress.endStageOk("extract", Map.of("entities", entityCount));

      List<Map<String, Object>> retrievedContextualData =
          oneMcp
              .graphService()
              .retrieveByContext(
                  assignmentContext.getContext().stream()
                      .map(c -> new GraphContextTuple(c.getEntity(), c.getOperations()))
                      .toList());

      // Plan generation stage
      progress.beginStage("plan", "Generating execution plan", 1);
      JsonNode plan =
          new PlanGenerationService(ctx).generatePlan(assignmentContext, retrievedContextualData);
      String planJson = JacksonUtility.toJson(plan);
      log.trace("Generated plan:\n{}", StringUtility.formatWithIndent(planJson, 4));
      int steps = plan.has("steps") && plan.get("steps").isArray() ? plan.get("steps").size() : 0;
      progress.endStageOk("plan", Map.of("steps", steps));

      // Prepare operations stage (we can’t know execution count precisely here)
      OperationRegistry operationRegistry = new OperationRegistry();
      for (Api api : ctx.handbook().apis().values()) {
        api.getService()
            .getInvokers()
            .forEach(
                (key, value) -> {
                  operationRegistry.register(
                      key,
                      (data) -> {
                        try {
                          long opStart = System.currentTimeMillis();
                          ctx.tracer().startChild("operation: %s.%s".formatted(api.getSlug(), key));
                          calledOperations.add("%s.%s".formatted(api.getSlug(), key));
                          log.trace(
                              "Invoking operation {} with data {}",
                              key,
                              JacksonUtility.toJson(data));
                          JsonNode result =
                              value.invoke(
                                  data.has("data") ? data.get("data") : data, toolCallContext);
                          long opEnd = System.currentTimeMillis();
                          ctx.tracer()
                              .endCurrentOk(
                                  Map.of(
                                      "service",
                                      api.getSlug(),
                                      "operation",
                                      key,
                                      "latencyMs",
                                      (opEnd - opStart)));
                          return result;
                        } catch (Exception e) {
                          log.error(
                              "Error invoking service {} with data {}",
                              key,
                              JacksonUtility.toJson(data),
                              e);
                          ctx.tracer()
                              .endCurrentError(
                                  Map.of(
                                      "service",
                                      api.getSlug(),
                                      "operation",
                                      key,
                                      "error",
                                      e.getClass().getSimpleName() + ": " + e.getMessage()));
                          throw new ExecutionPlanException(
                              "Error invoking service %s".formatted(key), e);
                        }
                      });
                });
      }

      // Execute plan stage
      progress.beginStage("exec", "Executing plan", calledOperations.size());
      ExecutionPlanEngine engine =
          new ExecutionPlanEngine(JacksonUtility.getJsonMapper(), operationRegistry);
      ctx.tracer().startChild("execution_plan");
      JsonNode output;
      try {
        output = engine.execute(plan, null);
        ctx.tracer().endCurrentOk(Map.of("engine", "ExecutionPlanEngine"));
        progress.endStageOk("exec", Map.of("engine", "ExecutionPlanEngine"));
      } catch (Exception ex) {
        ctx.tracer()
            .endCurrentError(
                Map.of("engine", "ExecutionPlanEngine", "error", ex.getClass().getSimpleName()));
        progress.endStageError(
            "exec", ex.getClass().getSimpleName(), Map.of("engine", "ExecutionPlanEngine"));
        throw ex;
      }
      String outputStr = JacksonUtility.toJson(output);

      log.trace("Generated answer:\n{}", StringUtility.formatWithIndent(outputStr, 4));
      if (assignmentContext.getUnhandledParts() != null
          && !assignmentContext.getUnhandledParts().isEmpty()) {
        assignmentParts.add(
            new AssigmentResult.Assignment(
                false, assignmentContext.getUnhandledParts(), false, null));
      }
      assignmentParts.add(
          new AssigmentResult.Assignment(
              true, assignmentContext.getRefinedAssignment(), false, outputStr));
    } catch (Exception e) {
      assignmentParts.add(
          new AssigmentResult.Assignment(
              true, prompt, true, ExceptionUtil.formatCompactStackTrace(e)));
      throw e;
    }

    long totalTimeMs = System.currentTimeMillis() - start;
    AssigmentResult.Statistics stats =
        new AssigmentResult.Statistics(
            ctx.tracer().promptTokens(),
            ctx.tracer().completionTokens(),
            ctx.tracer().totalTokens(),
            totalTimeMs,
            calledOperations,
            ctx.tracer().toTrace());
    progress.beginStage("finalize", "Finalizing response", 1);
    progress.endStageOk(
        "finalize",
        Map.of(
            "totalTimeMs",
            totalTimeMs,
            "promptTokens",
            ctx.tracer().promptTokens(),
            "completionTokens",
            ctx.tracer().completionTokens(),
            "totalTokens",
            ctx.tracer().totalTokens()));
    return new AssigmentResult(assignmentParts, stats, assignmentContext, null);
  }
}
