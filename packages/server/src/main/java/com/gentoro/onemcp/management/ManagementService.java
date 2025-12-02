package com.gentoro.onemcp.management;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.ValidationException;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.handbook.model.agent.Entity;
import com.gentoro.onemcp.handbook.model.agent.EntityOperation;
import com.gentoro.onemcp.handbook.model.regression.RegressionSuite;
import com.gentoro.onemcp.management.model.EntityGenerationResult;
import com.gentoro.onemcp.openapi.OpenApiCurator;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.CollectionUtility;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ManagementService {
  private final OneMcp oneMcp;

  public ManagementService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  private void reportError(
      String section,
      PromptTemplate.PromptSession promptSession,
      Map<String, Object> generalContext,
      String result,
      String errorDetails) {
    promptSession.enableOnly(
        section,
        CollectionUtility.mergeMaps(
            generalContext,
            CollectionUtility.mapOf(
                String.class,
                Object.class,
                "error_reported",
                true,
                "result",
                Objects.requireNonNullElse(result, ""),
                "error",
                errorDetails)));
  }

  public EntityGenerationResult generateEntities(Api api) {
    final String SECTION_ID = "entity-generation";
    String apiJsonContent;
    try {
      Map<String, Object> curated =
          OpenApiCurator.entityGenerationContext(api.getService().getOpenApi(), false);
      apiJsonContent =
          JacksonUtility.getJsonMapper()
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(curated);
    } catch (Exception e) {
      throw new ValidationException("Error while attempting to generate API curated definition", e);
    }

    Map<String, Object> generalContext =
        CollectionUtility.mapOf(
            String.class,
            Object.class,
            "curated_definition",
            apiJsonContent,
            "error_reported",
            false);

    int attempts = 0;
    PromptTemplate.PromptSession promptSession =
        oneMcp
            .promptRepository()
            .get("/entity-generation")
            .newSession()
            .enableOnly(SECTION_ID, generalContext);
    while (++attempts <= 3) {
      String result =
          oneMcp.llmClient().generate(promptSession.renderText(), List.of(), false, null);
      if (result == null || result.isBlank()) {
        reportError(
            SECTION_ID, promptSession, generalContext, result, "Did not produce a valid response");
        continue;
      }

      String jsonContent = StringUtility.extractSnippet(result, "json");
      if (jsonContent == null || jsonContent.isBlank()) {
        reportError(
            SECTION_ID, promptSession, generalContext, result, "No JSON snippet found in response");
        continue;
      }

      try {
        EntityGenerationResult genEntities =
            JacksonUtility.getJsonMapper().readValue(jsonContent, EntityGenerationResult.class);
        genEntities
            .getEntities()
            .forEach(
                genEntity -> {
                  final Entity apiEntity = new Entity(genEntity.getEntityName());
                  apiEntity.setDescription(genEntity.getDescription());

                  genEntity
                      .getOperations()
                      .forEach(
                          genOperation -> {
                            EntityOperation entityOperation = new EntityOperation();
                            entityOperation.setKind(genOperation.getOperationType());
                            entityOperation.setDescription(genOperation.getDefinition());
                            apiEntity.addOperation(entityOperation);
                          });

                  api.addEntity(apiEntity);
                });
        return genEntities;
      } catch (Exception e) {
        reportError(
            SECTION_ID,
            promptSession,
            generalContext,
            result,
            ExceptionUtil.formatCompactStackTrace(e));
      }
    }
    throw new ExecutionException("Failed to extract entities from assignment after 3 attempts");
  }

  public RegressionSuite generateRegressionSuite(Api api) {
    final String SECTION_ID = "regression-suite-generation";
    String apiJsonContent;
    try {
      Map<String, Object> curated =
          OpenApiCurator.entityGenerationContext(api.getService().getOpenApi(), true);
      apiJsonContent =
          JacksonUtility.getJsonMapper()
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(curated);
    } catch (Exception e) {
      throw new ValidationException("Error while attempting to generate API curated definition", e);
    }

    Map<String, Object> generalContext =
        CollectionUtility.mapOf(
            String.class,
            Object.class,
            "curated_definition",
            apiJsonContent,
            "error_reported",
            false);

    int attempts = 0;
    PromptTemplate.PromptSession promptSession =
        oneMcp
            .promptRepository()
            .get("/regression-suite-generation")
            .newSession()
            .enableOnly(SECTION_ID, generalContext);
    while (++attempts <= 3) {
      String result =
          oneMcp.llmClient().generate(promptSession.renderText(), List.of(), false, null);
      if (result == null || result.isBlank()) {
        reportError(
            SECTION_ID, promptSession, generalContext, result, "Did not produce a valid response");
        continue;
      }

      String jsonContent = StringUtility.extractSnippet(result, "json");
      if (jsonContent == null || jsonContent.isBlank()) {
        reportError(
            SECTION_ID, promptSession, generalContext, result, "No JSON snippet found in response");
        continue;
      }

      try {
        RegressionSuite regressionSuite =
            JacksonUtility.getJsonMapper().readValue(jsonContent, RegressionSuite.class);
        regressionSuite.setName("Tests for API: " + api.getName());
        regressionSuite.setVersion("0.0.1");
        if (Objects.nonNull(regressionSuite.getTests()) && !regressionSuite.getTests().isEmpty()) {
          if (regressionSuite.getTests().stream()
              .anyMatch(
                  testCase ->
                      (Objects.isNull(testCase.getPrompt()) || testCase.getPrompt().isEmpty())
                          || (Objects.isNull(testCase.getAssertion())
                              || testCase.getAssertion().isEmpty())
                          || (Objects.isNull(testCase.getDisplayName())
                              || testCase.getDisplayName().isEmpty()))) {
            throw new ValidationException(
                "Invalid regression suite definition, missing required properties (displayName, prompt or assertion)");
          }
        }
        return regressionSuite;
      } catch (Exception e) {
        reportError(
            SECTION_ID,
            promptSession,
            generalContext,
            result,
            ExceptionUtil.formatCompactStackTrace(e));
      }
    }
    throw new ExecutionException(
        "Failed to extract regression suite from assignment after 3 attempts");
  }
}
