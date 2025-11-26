package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.cache.DictionaryExtractorService;
import com.gentoro.onemcp.cache.PromptDictionary;
import com.gentoro.onemcp.cache.PromptSchemaNormalizer;
import com.gentoro.onemcp.cache.PromptSchemaWorkflow;
import com.gentoro.onemcp.context.KnowledgeBase;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.openapi.OpenApiLoader;
import com.gentoro.onemcp.openapi.OpenApiProxy;
import com.gentoro.onemcp.openapi.OpenApiProxyImpl;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OrchestratorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OrchestratorService.class);

  private enum Phase {
    EXTRACT,
    PLAN,
    EXECUTE,
    SUMMARY,
    COMPLETED
  }

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
            handlePrompt(input, true);
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
    return handlePrompt(prompt, false);
  }

  public String handlePrompt(String prompt, boolean interactive) {
    log.trace("Processing prompt: {}", prompt);

    // Generate unique execution ID and start tracking
    String executionId = java.util.UUID.randomUUID().toString();
    // Use inference logger (handles both report mode and normal logging)
    // Get report path pre-operation (determined at execution start)
    String reportPath = oneMcp.inferenceLogger().startExecution(executionId, prompt);

    OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore());
    
    EntityExtractionService entityExtraction = new EntityExtractionService(ctx);
    PlanGenerationService planGeneration = new PlanGenerationService(ctx);
    
    String summary = null;
    long start = System.currentTimeMillis();
    
    Phase currentPhase = Phase.EXTRACT;
    AssignmentContext assignmentContext = null;
    JsonNode executionPlan = null;
    JsonNode executionResult = null;
    
    while (currentPhase != Phase.COMPLETED) {
      switch (currentPhase) {
        case EXTRACT:
          oneMcp.inferenceLogger().logPhaseChange("extract");
          StdoutUtility.printNewLine(oneMcp, "Extracting entities and context.");
          assignmentContext = entityExtraction.extractContext(prompt.trim());
          currentPhase = Phase.PLAN;
          log.trace("Assignment context extracted: {}", assignmentContext);
          StdoutUtility.printSuccessLine(
              oneMcp,
              "Entity extraction completed in (%d ms)."
                  .formatted(System.currentTimeMillis() - start));
          break;
        case PLAN:
          oneMcp.inferenceLogger().logPhaseChange("plan");
          StdoutUtility.printNewLine(oneMcp, "Generating execution plan.");
          executionPlan = planGeneration.generatePlan(assignmentContext);
          currentPhase = Phase.EXECUTE;
          log.trace(
              "Execution plan:\n{}", StringUtility.formatWithIndent(executionPlan.toString(), 4));
          StdoutUtility.printSuccessLine(
              oneMcp,
              "Plan generation completed in (%d ms).\n%s"
                  .formatted(
                      (System.currentTimeMillis() - start),
                      StringUtility.formatWithIndent(executionPlan.toString(), 4)));
          break;
        case EXECUTE:
          oneMcp.inferenceLogger().logPhaseChange("execute");
          StdoutUtility.printNewLine(oneMcp, "Executing plan.");
          OperationRegistry registry = buildOperationRegistry(ctx);
          ExecutionPlanEngine engine = new ExecutionPlanEngine(
              JacksonUtility.getJsonMapper(), registry);
          executionResult = engine.execute(executionPlan, null);
          currentPhase = Phase.SUMMARY;
          log.trace("Execution result:\n{}", StringUtility.formatWithIndent(executionResult.toString(), 4));
          StdoutUtility.printSuccessLine(
              oneMcp,
              "Plan execution completed in (%d ms)."
                  .formatted(System.currentTimeMillis() - start));
          break;
        case SUMMARY:
          oneMcp.inferenceLogger().logPhaseChange("summary");
          StdoutUtility.printNewLine(oneMcp, "Compiling answer.");
          // Extract summary from execution result
          if (executionResult != null && executionResult.has("answer")) {
            summary = executionResult.get("answer").asText();
          } else if (executionResult != null) {
            // Fallback: convert result to string
            summary = executionResult.toString();
          } else {
            summary = "Execution completed but no result was produced.";
          }
          log.trace("Generated answer:\n{}", StringUtility.formatWithIndent(summary, 4));
          currentPhase = Phase.COMPLETED;
          StdoutUtility.printSuccessLine(
              oneMcp,
              "Assignment handled in (%s ms)\nAnswer: \n%s"
                  .formatted(
                      (System.currentTimeMillis() - start),
                      StringUtility.formatWithIndent(summary, 4)));
          break;
      }
    }

    // Normalize prompt schema in background if dictionary is available
    // Wait for normalization to complete before generating the report
    // Pass executionId explicitly since thread-local won't work in background thread
    final String normalizationExecutionId = executionId;
    final OrchestratorContext normalizationCtx = ctx; // Capture context for background thread
    
    // Capture handbook path from main thread before background thread starts
    // This ensures we use the actual handbook directory (from HANDBOOK_DIR env var or KnowledgeBase)
    // rather than relying on environment variable access in background thread
    Path capturedHandbookPath = null;
    try {
      // First try HANDBOOK_DIR environment variable (set by CLI)
      String envHandbookDir = System.getenv("HANDBOOK_DIR");
      if (envHandbookDir != null && !envHandbookDir.isBlank()) {
        Path envHandbookPath = Path.of(envHandbookDir);
        if (Files.exists(envHandbookPath) && Files.isDirectory(envHandbookPath)) {
          capturedHandbookPath = envHandbookPath;
          log.info("Captured HANDBOOK_DIR for normalization: {} (executionId: {})", capturedHandbookPath, normalizationExecutionId);
        } else {
          log.warn("HANDBOOK_DIR environment variable points to non-existent directory: {} (executionId: {})", envHandbookDir, normalizationExecutionId);
        }
      }
      
      // Fallback to KnowledgeBase path if HANDBOOK_DIR not set or invalid
      if (capturedHandbookPath == null) {
        KnowledgeBase knowledgeBase = ctx.knowledgeBase();
        if (knowledgeBase != null) {
          capturedHandbookPath = knowledgeBase.handbookPath();
          log.info("Captured KnowledgeBase handbook path for normalization: {} (executionId: {})", capturedHandbookPath, normalizationExecutionId);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to capture handbook path in main thread (executionId: {}): {}", normalizationExecutionId, e.getMessage());
      log.debug("Handbook path capture error details", e);
    }
    
    final Path finalHandbookPath = capturedHandbookPath; // Final variable for use in lambda
    
    CompletableFuture<Void> normalizationFuture = CompletableFuture.runAsync(() -> {
      log.info("Background normalization thread started (executionId: {})", normalizationExecutionId);
      // Set executionId in thread-local for logging in background thread
      oneMcp.inferenceLogger().setExecutionId(normalizationExecutionId);
      try {
        Path handbookPath = finalHandbookPath;
        
        if (handbookPath == null) {
          log.info("Skipping normalization: handbookPath is null (executionId: {})", normalizationExecutionId);
          // Log to InferenceLogger that handbookPath is null
          try {
            Map<String, String> errorData = new HashMap<>();
            errorData.put("error", "Handbook path is null - cannot load dictionary");
            errorData.put("reason", "handbook_path_null");
            String errorJson = JacksonUtility.getJsonMapper().writeValueAsString(errorData);
            oneMcp.inferenceLogger().logNormalizedPromptSchemaForExecution(errorJson, 0, normalizationExecutionId);
          } catch (Exception e) {
            log.debug("Failed to log normalization error to InferenceLogger", e);
          }
          return;
        }
        
        log.info("Using handbook path for dictionary lookup: {} (executionId: {})", handbookPath, normalizationExecutionId);
        
        // Try to load dictionary from apis/dictionary.yaml
        Path dictionaryPath = handbookPath.resolve("apis").resolve("dictionary.yaml");
        log.info("Checking for dictionary at: {} (executionId: {})", dictionaryPath, normalizationExecutionId);
        if (!Files.exists(dictionaryPath)) {
          log.info("Skipping normalization: dictionary file not found at: {} (executionId: {})", dictionaryPath, normalizationExecutionId);
          // Log to InferenceLogger that dictionary was not found
          try {
            Map<String, String> errorData = new HashMap<>();
            errorData.put("error", "Dictionary file not found at: " + dictionaryPath);
            errorData.put("reason", "dictionary_not_found");
            String errorJson = JacksonUtility.getJsonMapper().writeValueAsString(errorData);
            oneMcp.inferenceLogger().logNormalizedPromptSchemaForExecution(errorJson, 0, normalizationExecutionId);
          } catch (Exception e) {
            log.debug("Failed to log normalization error to InferenceLogger", e);
          }
          return;
        }
        
        long normalizeStart = System.currentTimeMillis();
        log.info("Starting background prompt schema normalization (executionId: {})", normalizationExecutionId);
        try {
          DictionaryExtractorService extractor = new DictionaryExtractorService(oneMcp);
          PromptDictionary dictionary = extractor.loadDictionary(dictionaryPath);
          if (dictionary != null) {
            log.debug("Dictionary loaded, normalizing prompt schema in background");
            PromptSchemaNormalizer normalizer = new PromptSchemaNormalizer(oneMcp);
            PromptSchemaWorkflow normalizedWorkflow = normalizer.normalize(prompt.trim(), dictionary);
            
            long normalizeDuration = System.currentTimeMillis() - normalizeStart;
            
            // Log normalized schema to report (use explicit executionId)
            String normalizedJson = JacksonUtility.getJsonMapper().writeValueAsString(normalizedWorkflow);
            oneMcp.inferenceLogger().logNormalizedPromptSchemaForExecution(normalizedJson, normalizeDuration, normalizationExecutionId);
            log.info("Background prompt schema normalization completed ({}ms, executionId: {})", normalizeDuration, normalizationExecutionId);
          } else {
            log.warn("Dictionary file exists but could not be loaded: {} (executionId: {})", dictionaryPath, normalizationExecutionId);
            // Log to InferenceLogger that dictionary could not be loaded
            try {
              Map<String, String> errorData = new HashMap<>();
              errorData.put("error", "Dictionary file exists but could not be loaded: " + dictionaryPath);
              errorData.put("reason", "dictionary_load_failed");
              String errorJson = JacksonUtility.getJsonMapper().writeValueAsString(errorData);
              oneMcp.inferenceLogger().logNormalizedPromptSchemaForExecution(errorJson, 0, normalizationExecutionId);
            } catch (Exception e) {
              log.debug("Failed to log normalization error to InferenceLogger", e);
            }
          }
        } catch (Exception normalizeError) {
          long normalizeDuration = System.currentTimeMillis() - normalizeStart;
          log.warn("Background normalization failed after {}ms (executionId: {}): {}", normalizeDuration, normalizationExecutionId, normalizeError.getMessage());
          log.debug("Normalization error details", normalizeError);
        }
      } catch (Exception e) {
        // Normalization is optional - log but don't fail execution
        log.warn("Failed to start background normalization (executionId: {}): {}", normalizationExecutionId, e.getMessage());
        log.debug("Normalization setup error details", e);
      }
    });

    // Wait for normalization to complete before generating the report
    // This ensures the normalized schema is included in the report
    try {
      // Wait up to 60 seconds for normalization to complete
      normalizationFuture.get(60, java.util.concurrent.TimeUnit.SECONDS);
      log.debug("Normalization completed, proceeding with report generation (executionId: {})", executionId);
    } catch (java.util.concurrent.TimeoutException e) {
      log.warn("Normalization timed out after 60 seconds, proceeding with report generation (executionId: {})", executionId);
    } catch (Exception e) {
      log.warn("Error waiting for normalization to complete, proceeding with report generation (executionId: {}): {}", executionId, e.getMessage());
      log.debug("Normalization wait error details", e);
    }

    // Log final response
    if (summary != null) {
      oneMcp.inferenceLogger().logFinalResponse(summary);
    }
    
    // Calculate execution metrics
    long durationMs = System.currentTimeMillis() - start;
    boolean success = summary != null && !summary.isEmpty();
    
    // Build and log AssigmentResult
    try {
      AssigmentResult assignmentResult = buildAssigmentResult(
          ctx, assignmentContext, executionPlan, durationMs, summary, success);
      oneMcp.inferenceLogger().logAssigmentResult(assignmentResult);
    } catch (Exception e) {
      log.warn("Failed to build or log AssigmentResult: {}", e.getMessage());
      log.debug("AssigmentResult build error details", e);
    }
    
    // Complete execution tracking
    String finalReportPath = oneMcp.inferenceLogger().completeExecution(executionId, durationMs, success);
    
    // Return JSON with content and reportPath if report is available
    if (finalReportPath != null && !finalReportPath.isEmpty()) {
      try {
        com.fasterxml.jackson.databind.ObjectMapper mapper = 
            com.gentoro.onemcp.utility.JacksonUtility.getJsonMapper();
        java.util.Map<String, String> response = new java.util.HashMap<>();
        response.put("content", summary != null ? summary : "");
        response.put("reportPath", finalReportPath);
        return mapper.writeValueAsString(response);
      } catch (Exception e) {
        log.warn("Failed to serialize response with report path, returning summary only", e);
        return summary;
      }
    }
    
    return summary;
  }

  /**
   * Build an OperationRegistry from KnowledgeBase services.
   * Each service's operations are registered as invokable functions.
   */
  private OperationRegistry buildOperationRegistry(OrchestratorContext ctx) {
    OperationRegistry registry = new OperationRegistry();
    KnowledgeBase kb = ctx.knowledgeBase();
    
    if (kb == null || kb.services() == null) {
      log.warn("No knowledge base or services available for operation registry");
      return registry;
    }
    
    // Get base URL from configuration (default to localhost:8080)
    String baseUrl = oneMcp.configuration().getString("server.baseUrl", "http://localhost:8080");
    int operationCount = 0;
    
    for (Service service : kb.services()) {
      try {
        // Load OpenAPI spec for this service
        io.swagger.v3.oas.models.OpenAPI openAPI = service.definition(kb.handbookPath());
        
        // Build OpenAPI proxy to handle operation invocations
        OpenApiProxy proxy = new OpenApiProxyImpl(openAPI, baseUrl);
        
        // Register each operation from this service
        for (com.gentoro.onemcp.context.Operation op : service.getOperations()) {
          String operationId = op.getOperation();
          registry.register(operationId, (input) -> {
            try {
              return proxy.invoke(operationId, input);
            } catch (Exception e) {
              log.error("Failed to invoke operation: {}", operationId, e);
              throw new ExecutionPlanException("Operation '" + operationId + "' failed: " + e.getMessage(), e);
            }
          });
          operationCount++;
          log.debug("Registered operation: {}", operationId);
        }
      } catch (Exception e) {
        log.warn("Failed to register operations for service: {}", service.getSlug(), e);
      }
    }
    
    log.info("Built operation registry with {} operations", operationCount);
    return registry;
  }

  /**
   * Build an AssigmentResult from execution data.
   *
   * @param ctx orchestrator context with telemetry tracer
   * @param assignmentContext the extracted assignment context
   * @param executionPlan the execution plan JSON
   * @param durationMs total execution duration
   * @param summary the final summary/answer
   * @param success whether execution succeeded
   * @return the constructed AssigmentResult
   */
  private AssigmentResult buildAssigmentResult(
      OrchestratorContext ctx,
      AssignmentContext assignmentContext,
      JsonNode executionPlan,
      long durationMs,
      String summary,
      boolean success) {
    
    // Build assignment parts and extract operations from execution plan in one pass
    List<AssigmentResult.Assignment> parts = new ArrayList<>();
    List<String> operationsList = new ArrayList<>();
    Set<String> seenOperations = new HashSet<>();
    
    if (executionPlan != null && executionPlan.isObject()) {
      executionPlan.fields().forEachRemaining(entry -> {
        String nodeId = entry.getKey();
        if ("start_node".equals(nodeId)) {
          return; // Skip start node
        }
        JsonNode nodeDef = entry.getValue();
        if (nodeDef != null && nodeDef.isObject()) {
          JsonNode operation = nodeDef.get("operation");
          if (operation != null && operation.isTextual()) {
            String opName = operation.asText();
            // Add to operations list if not already seen
            if (!seenOperations.contains(opName)) {
              operationsList.add(opName);
              seenOperations.add(opName);
            }
            // Create assignment part for each operation
            boolean isError = nodeDef.has("error") || 
                (nodeDef.has("completed") && !nodeDef.get("completed").asBoolean());
            String content = nodeDef.has("vars") ? 
                nodeDef.get("vars").toString() : "";
            parts.add(new AssigmentResult.Assignment(
                true, // isSupported - assume true if in plan
                opName,
                isError,
                content));
          }
        }
      });
    }
    
    // If no parts from plan, create a single part from the summary
    if (parts.isEmpty() && summary != null) {
      parts.add(new AssigmentResult.Assignment(
          true,
          "assignment",
          !success,
          summary));
    }
    
    // Build statistics from telemetry tracer
    TelemetryTracer tracer = ctx.tracer();
    
    AssigmentResult.Statistics statistics = new AssigmentResult.Statistics(
        tracer.promptTokens(),
        tracer.completionTokens(),
        tracer.totalTokens(),
        durationMs,
        operationsList,
        tracer.toTrace());
    
    // Use assignment context (may be null if extraction failed)
    AssignmentContext context = assignmentContext != null ? 
        assignmentContext : new AssignmentContext();
    
    return new AssigmentResult(parts, statistics, context);
  }
}
