package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.openapi.EndpointInvoker;
import com.gentoro.onemcp.openapi.OpenApiLoader;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.OpenAPI;
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

  public String handlePrompt(String prompt) {
    log.trace("Processing prompt: {}", prompt);

    long start = System.currentTimeMillis();
    try {
      OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore());
      StdoutUtility.printNewLine(oneMcp, "Extracting entities.");
      AssignmentContext assignmentContext =
          new EntityExtractionService(ctx).extractContext(prompt.trim());

      StdoutUtility.printNewLine(oneMcp, "Generating execution plan.");
      JsonNode plan = new PlanGenerationService(ctx).generatePlan(assignmentContext);
      StdoutUtility.printSuccessLine(
          oneMcp,
          "Generated plan:\n%s"
              .formatted(StringUtility.formatWithIndent(JacksonUtility.toJson(plan), 4)));
      log.trace(
          "Generated plan:\n{}", StringUtility.formatWithIndent(JacksonUtility.toJson(plan), 4));

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
                      log.trace(
                          "Invoking operation {} with data {}", key, JacksonUtility.toJson(data));
                      StdoutUtility.printRollingLine(
                          oneMcp, "Invoking operation %s".formatted(key));
                      return value.invoke(data.has("data") ? data.get("data") : data);
                    } catch (Exception e) {
                      log.error(
                          "Error invoking service {} with data {}",
                          key,
                          JacksonUtility.toJson(data),
                          e);
                      throw new ExecutionPlanException(
                          "Error invoking service %s".formatted(key), e);
                    }
                  });
            });
      }

      StdoutUtility.printNewLine(oneMcp, "Executing plan.");
      ExecutionPlanEngine engine =
          new ExecutionPlanEngine(JacksonUtility.getJsonMapper(), operationRegistry);
      JsonNode output = engine.execute(plan, null);
      String outputStr = JacksonUtility.toJson(output);

      log.trace("Generated answer:\n{}", StringUtility.formatWithIndent(outputStr, 4));
      StdoutUtility.printSuccessLine(
          oneMcp,
          "Assignment handled in (%s ms)\nAnswer: \n%s"
              .formatted(
                  (System.currentTimeMillis() - start),
                  StringUtility.formatWithIndent(outputStr, 4)));
      return outputStr;
    } catch (Exception e) {
      StdoutUtility.printError(oneMcp, "Could not handle assignment properly", e);
      throw e;
    }
  }
}
