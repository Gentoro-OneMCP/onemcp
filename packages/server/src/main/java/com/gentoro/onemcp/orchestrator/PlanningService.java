package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.messages.ExecutionPlan;
import com.gentoro.onemcp.model.LlmClient;

import java.util.ArrayList;
import java.util.List;

/**
 * PlanningService is responsible for transforming the user's assignment into an executable {@link
 * ExecutionPlan} by prompting the LLM with the planning get.
 */
public class PlanningService extends AbstractServiceImpl<ExecutionPlan> {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(PlanningService.class);

  public PlanningService(OrchestratorContext ctx) {
    super(ctx, ExecutionPlan.class);
  }

  /**
   * Produce an execution plan for the given natural-language assignment.
   *
   * @param assignment user request/assignment
   * @return structured execution plan
   */
  public ExecutionPlan plan(String assignment) {
    log.trace("Planing execution for prompt: {}", assignment);

    super.newSession("/plan")
        .enableOnly(
            "handshake",
            "userAssignment",
            assignment.trim(),
            "acmeHandbook",
            context().oneMcp().knowledgeBase().getDocument("kb:///instructions.md"),
            "services",
            context().oneMcp().knowledgeBase().services());
    int attempts = 0;
    while(++attempts <= 3) {
      ++attempts;
      ExecutionPlan executionPlan = super.chat();
        if( executionPlan == null || executionPlan.steps() == null || executionPlan.steps().isEmpty() ) {
          log.warn("Execution plan is empty, retrying");
            enable("empty-plan");
        } else {
          return executionPlan;
        }
    }

    throw new ExecutionException("Could not produce a valid execution plan after %d attempts.".formatted(attempts));
  }
}
