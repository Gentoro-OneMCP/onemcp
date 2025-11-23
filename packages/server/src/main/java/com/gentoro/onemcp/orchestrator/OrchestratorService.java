package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.openapi.EndpointInvoker;
import com.gentoro.onemcp.openapi.OpenApiLoader;
import com.gentoro.onemcp.orchestrator.progress.NoOpProgressSink;
import com.gentoro.onemcp.orchestrator.progress.ProgressSink;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.OpenAPI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class OrchestratorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OrchestratorService.class);

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
            handlePrompt(input);
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

  public AssigmentResult handlePrompt(String prompt) {
    return handlePrompt(prompt, new NoOpProgressSink());
  }

  /**
   * Handle a natural language prompt request and return a structured result, emitting optional
   * progress updates through the provided {@link ProgressSink}.
   */
  public AssigmentResult handlePrompt(String prompt, ProgressSink progress) {
    log.trace("Processing prompt: {}", prompt);

    final List<String> calledOperations = new ArrayList<>();
    final List<AssigmentResult.Assignment> assignmentParts = new ArrayList<>();
    final long start = System.currentTimeMillis();
    AssignmentContext assignmentContext = null;
    OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore());
    try {
      // annotate root span with incoming prompt (truncated to avoid sensitive data)
      TelemetryTracer tracer = ctx.tracer();
      if (tracer.current() != null) {
        String promptPreview = prompt.length() > 500 ? prompt.substring(0, 500) + "…" : prompt;
        tracer.current().attributes.put("prompt.preview", promptPreview);
      }

      // Extract entities stage
      progress.beginStage("extract", "Extracting entities", 1);
      StdoutUtility.printNewLine(oneMcp, "Extracting entities.");
      assignmentContext = new EntityExtractionService(ctx).extractContext(prompt.trim());
      int entityCount =
          assignmentContext.getContext() == null ? 0 : assignmentContext.getContext().size();
      progress.endStageOk("extract", Map.of("entities", entityCount));

      // Plan generation stage
      progress.beginStage("plan", "Generating execution plan", 1);
      StdoutUtility.printNewLine(oneMcp, "Generating execution plan.");
      JsonNode plan = new PlanGenerationService(ctx).generatePlan(assignmentContext);
      StdoutUtility.printSuccessLine(
          oneMcp,
          "Generated plan:\n%s"
              .formatted(StringUtility.formatWithIndent(JacksonUtility.toJson(plan), 4)));
      log.trace(
          "Generated plan:\n{}", StringUtility.formatWithIndent(JacksonUtility.toJson(plan), 4));
      int steps = plan.has("steps") && plan.get("steps").isArray() ? plan.get("steps").size() : 0;
      progress.endStageOk("plan", Map.of("steps", steps));

      // Prepare operations stage (we can’t know execution count precisely here)
      OperationRegistry operationRegistry = new OperationRegistry();
      for (Service service : ctx.knowledgeBase().services()) {
        OpenAPI openApiDef = service.definition(ctx.knowledgeBase().handbookPath());
        Map<String, EndpointInvoker> invokerMap =
            OpenApiLoader.buildInvokers(openApiDef, openApiDef.getServers().getFirst().getUrl());
        invokerMap.forEach(
            (key, value) -> {
              operationRegistry.register(
                  key,
                  (data) -> {
                    try {
                      long opStart = System.currentTimeMillis();
                      ctx.tracer().startChild("operation: %s.%s".formatted(service.getSlug(), key));
                      calledOperations.add("%s.%s".formatted(service.getSlug(), key));
                      log.trace(
                          "Invoking operation {} with data {}", key, JacksonUtility.toJson(data));
                      StdoutUtility.printRollingLine(
                          oneMcp, "Invoking operation %s".formatted(key));
                      JsonNode result = value.invoke(data.has("data") ? data.get("data") : data);
                      long opEnd = System.currentTimeMillis();
                      ctx.tracer()
                          .endCurrentOk(
                              Map.of(
                                  "service",
                                  service.getSlug(),
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
                                  service.getSlug(),
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
      StdoutUtility.printNewLine(oneMcp, "Executing plan.");
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
      StdoutUtility.printSuccessLine(
          oneMcp,
          "Assignment handled in (%s ms)\nAnswer: \n%s"
              .formatted(
                  (System.currentTimeMillis() - start),
                  StringUtility.formatWithIndent(outputStr, 4)));

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
      StdoutUtility.printError(oneMcp, "Could not handle assignment properly", e);
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
    return new AssigmentResult(assignmentParts, stats, assignmentContext);
  }
}
