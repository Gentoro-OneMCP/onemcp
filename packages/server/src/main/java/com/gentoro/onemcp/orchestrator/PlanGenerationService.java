package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.engine.ExecutionPlanValidator;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.List;
import java.util.Map;

public class PlanGenerationService {
  private final OrchestratorContext context;

  public PlanGenerationService(OrchestratorContext context) {
    this.context = context;
  }

  public JsonNode generatePlan(AssignmentContext assignmentContext) {
    int attempts = 0;
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
                    "error_reported",
                    false));
    while (++attempts <= 3) {
      String result =
          context.llmClient().chat(promptSession.renderMessages(), List.of(), false, null);
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
            context.knowledgeBase().services().stream()
                .flatMap(s -> s.getOperations().stream())
                .map(o -> o.getOperation())
                .toList();

        JsonNode executionPlan = JacksonUtility.getJsonMapper().readTree(jsonContent);
        ExecutionPlanValidator.validate(executionPlan, listOfAllowedOperations);
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
      }
    }

    throw new ExecutionException(
        "Failed to generate execution plan from assignment after 3 attempts");
  }
}
