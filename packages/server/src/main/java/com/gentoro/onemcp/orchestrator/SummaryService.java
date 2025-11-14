package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.messages.Summary;

/** SummaryService asks the LLM to produce a human-friendly summary of the work done. */
public class SummaryService extends AbstractServiceImpl<Summary> {
  public SummaryService(OrchestratorContext ctx) {
    super(ctx, Summary.class);
  }

  /**
   * Create a structured {@link Summary} for the given assignment, including any variables produced
   * during execution.
   */
  public Summary summarize(String assignment) {
    return super.newSession("/summary")
        .enableOnly(
            "summary",
            "assignment",
            assignment.trim(),
            "variables",
            context().memory().list().values())
        .chat();
  }
}
