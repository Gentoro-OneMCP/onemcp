package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.List;
import java.util.Map;

public class EntityExtractionService {
  private final OrchestratorContext context;

  public EntityExtractionService(OrchestratorContext context) {
    this.context = context;
  }

  public AssignmentContext extractContext(String assignment) {
    int attempts = 0;
    PromptTemplate.PromptSession promptSession =
        context
            .prompts()
            .get("/entity_extraction")
            .newSession()
            .enableOnly("assignment", Map.of("assignment", assignment, "error_reported", false));
    while (++attempts <= 3) {
      String result =
          context.llmClient().chat(promptSession.renderMessages(), List.of(), false, null);
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
      }
    }

    throw new ExecutionException("Failed to extract entities from assignment after 3 attempts");
  }
}
