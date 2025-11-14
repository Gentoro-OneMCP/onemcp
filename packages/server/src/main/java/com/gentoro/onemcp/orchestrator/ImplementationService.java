package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.exception.StateException;
import com.gentoro.onemcp.messages.ExecutionPlan;
import com.gentoro.onemcp.messages.StepImplementation;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ImplementationService is responsible for producing a concrete step implementation (code snippet)
 * for a given plan step by prompting the LLM using the implementation get. It optionally
 * incorporates details from previous attempts and from the shared memory.
 */
public class ImplementationService extends AbstractServiceImpl<StepImplementation> {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ImplementationService.class);

  public ImplementationService(OrchestratorContext ctx) {
    super(ctx, StepImplementation.class);
  }

  /**
   * Ask the LLM to produce a {@link StepImplementation} for the given {@link ExecutionPlan.Step}.
   * When {@code attempt} is provided, it is used to inform the model of the previous snippet and
   * error message to improve the next attempt.
   */
  public StepImplementation designStep(ExecutionPlan.Step step, String[] attempt) {
    log.trace("Design Step:\n{}", StringUtility.formatWithIndent(step.toString(), 4));
    List<Map<String, Object>> localContext = new ArrayList<>();
    for (ExecutionPlan.Step.Service stepSrv : step.services()) {
      Service service =
          context().oneMcp().knowledgeBase().services().stream()
              .filter(srv -> srv.getSlug().equals(stepSrv.serviceName()))
              .findFirst()
              .orElseThrow(
                  () -> {
                    log.error(
                        "Step requires a service that is not listed: {}", stepSrv.serviceName());
                    return new StateException(
                        "Step requires a service that is not listed: " + stepSrv.serviceName());
                  });

      localContext.add(
          Map.of(
              "service",
              service,
              "operations",
              service.getOperations().stream()
                  .filter(op -> stepSrv.operations().contains(op.getOperation()))
                  .toList()));
    }

    return super.newSession("/implement")
        .enableOnly(
            "implementation",
            "stepDescription",
            step.description().trim(),
            "instructions",
            context().oneMcp().knowledgeBase().getDocument("kb:///instructions.md"),
            "services",
            localContext)
        .enableIf(
            context().memory() != null && !context().memory().list().isEmpty(),
            "memory",
            "memory",
            context().memory().list().values())
        .enableIf(
            attempt != null,
            "attempt",
            "snippet",
            (attempt != null ? attempt[0] : null),
            "errors",
            (attempt != null ? attempt[1] : null))
        .chat();
  }
}
