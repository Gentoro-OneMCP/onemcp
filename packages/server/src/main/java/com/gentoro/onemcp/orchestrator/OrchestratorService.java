package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.plan.ExecutionPlan;
import com.gentoro.onemcp.cache.ExecutionPlanCache;
import com.gentoro.onemcp.cache.PromptLexicon;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.PromptSchemaNormalizer;
import com.gentoro.onemcp.cache.PromptSchemaWorkflow;
import com.gentoro.onemcp.cache.LexiconExtractorService;
import com.gentoro.onemcp.orchestrator.progress.NoOpProgressSink;
import com.gentoro.onemcp.orchestrator.progress.ProgressSink;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class OrchestratorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OrchestratorService.class);

  private final OneMcp oneMcp;
  private final boolean cacheEnabled;

  public OrchestratorService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    
    // Read cache enabled setting from configuration
    // The config should already have the env var interpolated: ${env:CACHE_ENABLED:-false}
    boolean configValue = oneMcp.configuration().getBoolean("orchestrator.cache.enabled", false);
    
    // Also check environment variable directly as primary source (since config interpolation might not work)
    // This ensures we respect the CACHE_ENABLED env var that the CLI sets
    String cacheEnabledEnv = System.getenv("CACHE_ENABLED");
    if (cacheEnabledEnv != null && !cacheEnabledEnv.trim().isEmpty()) {
      // Environment variable takes precedence (CLI sets this explicitly)
      boolean envValue = "true".equalsIgnoreCase(cacheEnabledEnv.trim());
      this.cacheEnabled = envValue;
      log.info("Orchestrator cache enabled: {} (from CACHE_ENABLED env var, config was: {})", 
          this.cacheEnabled, configValue);
    } else {
      // No env var set, use config value
      this.cacheEnabled = configValue;
      log.info("Orchestrator cache enabled: {} (from config orchestrator.cache.enabled, env var not set)", 
          this.cacheEnabled);
    }
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
   * 
   * <p>Routes to either:
   * <ul>
   *   <li>Legacy path: Uses orchestrator.PlanGenerationService (current implementation)
   *   <li>Caching path: Uses plan.PlanGenerationService + cache package (when orchestrator.cache.enabled=true)
   * </ul>
   */
  public AssigmentResult handlePrompt(String prompt, ProgressSink progress) {
    log.trace("Processing prompt: {}", prompt);

    if (oneMcp.handbook().apis().isEmpty()) {
      throw new HandbookException(
          "Cannot handle assignments just yet, there are no APIs loaded. "
              + "Review your handbook configuration and publish your APIs.");
    }

    // Use the cache enabled setting determined at startup
    if (cacheEnabled) {
      return handlePromptWithCache(prompt, progress);
    } else {
      return handlePromptLegacy(prompt, progress);
    }
  }

  /**
   * Legacy path: Uses orchestrator.PlanGenerationService (unchanged implementation).
   */
  private AssigmentResult handlePromptLegacy(String prompt, ProgressSink progress) {

    // Generate execution ID and start execution tracking
    String executionId = UUID.randomUUID().toString();
    String reportPath = oneMcp.inferenceLogger().startExecution(executionId, prompt);

    final List<String> calledOperations = new ArrayList<>();
    final List<AssigmentResult.Assignment> assignmentParts = new ArrayList<>();
    final long start = System.currentTimeMillis();
    AssignmentContext assignmentContext = null;
    OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore());
    boolean success = false;
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

      List<Map<String, Object>> retrievedContextualData =
          oneMcp
              .graphService()
              .retrieveByContext(
                  assignmentContext.getContext().stream()
                      .map(c -> new GraphContextTuple(c.getEntity(), c.getOperations()))
                      .toList());

      // Plan generation stage
      progress.beginStage("plan", "Generating execution plan", 1);
      StdoutUtility.printNewLine(oneMcp, "Generating execution plan.");
      JsonNode plan =
          new PlanGenerationService(ctx).generatePlan(assignmentContext, retrievedContextualData);
      String planJson = JacksonUtility.toJson(plan);
      StdoutUtility.printSuccessLine(
          oneMcp, "Generated plan:\n%s".formatted(StringUtility.formatWithIndent(planJson, 4)));
      log.trace("Generated plan:\n{}", StringUtility.formatWithIndent(planJson, 4));
      int steps = plan.has("steps") && plan.get("steps").isArray() ? plan.get("steps").size() : 0;
      progress.endStageOk("plan", Map.of("steps", steps));

      // Log execution plan for reporting (caching disabled, so no cache status)
      oneMcp.inferenceLogger().logExecutionPlan(planJson, null);

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
                          StdoutUtility.printRollingLine(
                              oneMcp, "Invoking operation %s".formatted(key));

                          // Set inference logger on invoker to log API calls
                          value.setInferenceLogger(oneMcp.inferenceLogger());
                          JsonNode result =
                              value.invoke(data.has("data") ? data.get("data") : data);
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
      success = true;

      // Log final response
      oneMcp.inferenceLogger().logFinalResponse(outputStr);
    } catch (Exception e) {
      StdoutUtility.printError(oneMcp, "Could not handle assignment properly", e);
      assignmentParts.add(
          new AssigmentResult.Assignment(
              true, prompt, true, ExceptionUtil.formatCompactStackTrace(e)));
      success = false;
      throw e;
    }

    // Build the result before completing execution so we can log it
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
    AssigmentResult result = new AssigmentResult(assignmentParts, stats, assignmentContext);
    
    // Log the assignment result for the report (before completing execution)
    oneMcp.inferenceLogger().logAssigmentResult(result);
    
    // Complete execution tracking (this generates the report)
    oneMcp.inferenceLogger().completeExecution(executionId, totalTimeMs, success);
    
    return result;
  }

  /**
   * Caching path: Uses plan.PlanGenerationService + cache package.
   * 
   * <p>Flow: Prompt → Normalize → PromptSchema → Cache lookup → Plan generation → Execute → Cache result
   */
  private AssigmentResult handlePromptWithCache(String prompt, ProgressSink progress) {
    // Generate execution ID and start execution tracking
    String executionId = UUID.randomUUID().toString();
    String reportPath = oneMcp.inferenceLogger().startExecution(executionId, prompt);

    final List<String> calledOperations = new ArrayList<>();
    final List<AssigmentResult.Assignment> assignmentParts = new ArrayList<>();
    final long start = System.currentTimeMillis();
    boolean success = false;
    
    // Get handbook path
    Path handbookPath = oneMcp.handbook().location();
    if (handbookPath == null) {
      throw new HandbookException("Handbook location is not available");
    }

    try {
      // 1. Load or generate lexicon
      progress.beginStage("normalize", "Normalizing prompt", 1);
      StdoutUtility.printNewLine(oneMcp, "Normalizing prompt to schema.");
      
      PromptLexicon lexicon;
      try {
        lexicon = loadOrGenerateLexicon(handbookPath);
      } catch (HandbookException e) {
        // Re-throw HandbookException as-is (it already has a good error message)
        throw e;
      } catch (Exception e) {
        // Wrap other exceptions
        throw new HandbookException(
            "Failed to load or generate lexicon: " + e.getMessage(), e);
      }
      if (lexicon == null) {
        throw new HandbookException(
            "Failed to load or generate lexicon. Please check logs for details.");
      }

      // 2. Normalize prompt to PromptSchema
      PromptSchemaNormalizer normalizer = new PromptSchemaNormalizer(oneMcp);
      PromptSchemaWorkflow workflow = normalizer.normalize(prompt, lexicon);
      
      // For now, handle single-step workflows (first step)
      if (workflow.getSteps() == null || workflow.getSteps().isEmpty()) {
        throw new ExecutionException("Normalization produced no steps");
      }
      PromptSchema schema = workflow.getSteps().get(0).getPs();
      if (schema == null) {
        throw new ExecutionException("First step has no PromptSchema");
      }
      
      log.info("Normalized prompt to schema with cache key: {}", schema.getCacheKey());
      progress.endStageOk("normalize", Map.of("cacheKey", schema.getCacheKey()));

      // 3. Check cache for existing plan
      ExecutionPlan plan = null;
      boolean cacheHit = false;
      Optional<ExecutionPlanCache.CachedPlanResult> cachedResult = Optional.empty();
      ExecutionPlanCache cache = new ExecutionPlanCache(handbookPath);
      
      progress.beginStage("cache", "Checking cache", 1);
      
      cachedResult = cache.lookupWithMetadata(schema);
      if (cachedResult.isPresent()) {
        cacheHit = true;
        plan = cachedResult.get().plan;
        log.info("Cache HIT for PSK: {}", schema.getCacheKey());
        if (!cachedResult.get().executionSucceeded && cachedResult.get().failedAttempts > 0) {
          log.warn("Cached plan previously failed ({} attempts). Will retry execution.", 
              cachedResult.get().failedAttempts);
        }
      } else {
        log.info("Cache MISS for PSK: {}", schema.getCacheKey());
      }
      progress.endStageOk("cache", Map.of("hit", cacheHit));

      // 4. Generate plan if cache miss (with retry support)
      String executionError = null;
      if (cachedResult.isPresent() && !cachedResult.get().executionSucceeded) {
        executionError = cachedResult.get().lastError;
      }
      
      if (plan == null) {
        progress.beginStage("plan", "Generating execution plan", 1);
        StdoutUtility.printNewLine(oneMcp, "Generating execution plan.");
        
        com.gentoro.onemcp.plan.PlanContext planContext = 
            new com.gentoro.onemcp.plan.PlanContext(oneMcp, new ValueStore());
        com.gentoro.onemcp.plan.PlanGenerationService planGenerator = 
            new com.gentoro.onemcp.plan.PlanGenerationService(planContext);
        
        plan = planGenerator.generatePlan(prompt, schema, executionError);
        log.info("Generated plan for PSK: {}", schema.getCacheKey());
        progress.endStageOk("plan", Map.of("steps", "generated"));
        
        // CRITICAL: Cache the plan IMMEDIATELY after generation, before execution
        // The plan must contain placeholders ({{params.X}}) and JsonPath expressions ($.['node'].['field'])
        // These will be hydrated/applied at execution time with values from the PromptSchema
        cache.store(schema, plan, false, null); // Store as pending (will be updated after execution)
        log.debug("Cached newly generated plan (with placeholders) for PSK: {}", schema.getCacheKey());
      }

      // Log execution plan for reporting (whether from cache or newly generated)
      // CRITICAL: Always log the plan so it appears in the report
      if (plan == null) {
        log.error("Cannot log execution plan: plan is null for PSK: {}", schema.getCacheKey());
        throw new ExecutionException("Execution plan is null - cannot proceed");
      }
      
      try {
        String planJson = plan.toJson();
        if (planJson == null || planJson.trim().isEmpty()) {
          log.error("Execution plan JSON is null or empty for PSK: {}", schema.getCacheKey());
          planJson = "{\"error\": \"Plan serialization failed\"}";
        }
        oneMcp.inferenceLogger().logExecutionPlan(planJson, cacheHit);
        log.debug("Logged execution plan for PSK: {} (cache hit: {}, length: {})", 
            schema.getCacheKey(), cacheHit, planJson.length());
      } catch (Exception e) {
        log.error("Failed to log execution plan for PSK: {}", schema.getCacheKey(), e);
        // Try to log an error message as the plan
        try {
          oneMcp.inferenceLogger().logExecutionPlan(
              "{\"error\": \"Failed to serialize plan: " + e.getMessage() + "\"}");
        } catch (Exception e2) {
          log.error("Failed to log error plan", e2);
        }
        // Continue execution even if logging fails
      }

      // 5. Execute plan
      progress.beginStage("exec", "Executing plan", 1);
      StdoutUtility.printNewLine(oneMcp, "Executing plan.");
      
      // Build operation registry
      OperationRegistry operationRegistry = new OperationRegistry();
      for (Api api : oneMcp.handbook().apis().values()) {
        api.getService()
            .getInvokers()
            .forEach(
                (key, value) -> {
                  operationRegistry.register(
                      key,
                      (data) -> {
                        try {
                          calledOperations.add("%s.%s".formatted(api.getSlug(), key));
                          StdoutUtility.printRollingLine(
                              oneMcp, "Invoking operation %s".formatted(key));
                          
                          // Set inference logger on invoker to log API calls
                          value.setInferenceLogger(oneMcp.inferenceLogger());
                          JsonNode result =
                              value.invoke(data.has("data") ? data.get("data") : data);
                          return result;
                        } catch (Exception e) {
                          log.error("Error invoking service {} with data {}", key, JacksonUtility.toJson(data), e);
                          throw new ExecutionPlanException(
                              "Error invoking service %s".formatted(key), e);
                        }
                      });
                });
      }

      // Execute the plan with retry logic for API failures
      String outputStr = null;
      boolean executionRetried = false;
      int maxExecutionRetries = 2; // Initial attempt + 1 retry
      
      for (int execAttempt = 1; execAttempt <= maxExecutionRetries; execAttempt++) {
        try {
          outputStr = plan.execute(schema, operationRegistry);
          if (outputStr == null || outputStr.trim().isEmpty()) {
            log.error("Plan execution returned null or empty result for PSK: {}", schema.getCacheKey());
            log.debug("Plan was from cache: {}", cacheHit && execAttempt == 1);
            throw new ExecutionException("Plan execution returned null or empty result");
          }
          success = true;
          progress.endStageOk("exec", Map.of("result", "success"));
          
          // Log execution result with the current plan
          try {
            String planJson = plan.toJson();
            if (planJson != null && !planJson.trim().isEmpty()) {
              oneMcp.inferenceLogger().logExecutionPlan(planJson, outputStr, null);
            }
          } catch (Exception logErr) {
            log.warn("Failed to log plan execution result", logErr);
          }
          
          break; // Success - exit retry loop
        } catch (Exception e) {
          // Extract just the error message for user-facing display (no stack trace)
          String errorMsg = ExceptionUtil.extractErrorMessage(e);
          // Use full stack trace for logging
          String fullError = ExceptionUtil.formatCompactStackTrace(e);
          log.error("Plan execution failed for PSK: {} (attempt {}/{}, cache hit: {})", 
              schema.getCacheKey(), execAttempt, maxExecutionRetries, cacheHit && execAttempt == 1, e);
          
          // Log execution error with the current plan (use clean error message, not stack trace)
          try {
            String planJson = plan.toJson();
            if (planJson != null && !planJson.trim().isEmpty()) {
              oneMcp.inferenceLogger().logExecutionPlan(planJson, null, errorMsg);
            }
          } catch (Exception logErr) {
            log.warn("Failed to log plan execution error", logErr);
          }
          
          // If this is not the last attempt and error suggests API failure, retry with new plan
          // Check for: API errors, HTTP 400/4xx/5xx status codes, or API call failures
          boolean isApiError = errorMsg.contains("API error:") || 
                               errorMsg.contains("API call failed") ||
                               errorMsg.matches(".*HTTP [45]\\d\\d.*"); // Matches HTTP 400-599
          
          if (execAttempt < maxExecutionRetries && isApiError) {
            log.info("API failure detected (HTTP error or API error). Regenerating plan with error feedback (attempt {})", execAttempt);
            executionError = errorMsg;
            executionRetried = true;
            
            // Regenerate plan with error feedback
            progress.beginStage("plan", "Regenerating plan after API failure", 1);
            com.gentoro.onemcp.plan.PlanContext planContext = 
                new com.gentoro.onemcp.plan.PlanContext(oneMcp, new ValueStore());
            com.gentoro.onemcp.plan.PlanGenerationService planGenerator = 
                new com.gentoro.onemcp.plan.PlanGenerationService(planContext);
            
            plan = planGenerator.generatePlan(prompt, schema, executionError);
            cacheHit = false; // Next attempt uses new plan
            log.info("Regenerated plan for PSK: {} (retry attempt {})", schema.getCacheKey(), execAttempt + 1);
            progress.endStageOk("plan", Map.of("steps", "regenerated", "retry", execAttempt + 1));
            
            // Re-log the new plan (without result yet - will be logged after execution)
            try {
              String planJson = plan.toJson();
              if (planJson != null && !planJson.trim().isEmpty()) {
                oneMcp.inferenceLogger().logExecutionPlan(planJson, false); // Regenerated plan is always a cache miss
              }
            } catch (Exception logErr) {
              log.warn("Failed to log regenerated plan", logErr);
            }
            
            progress.endStageError("exec", e.getClass().getSimpleName(), Map.of("retrying", true));
            continue; // Retry with new plan
          } else {
            // Last attempt failed or non-API error - cache failure and throw
            success = false;
            cache.store(schema, plan, false, errorMsg);
            progress.endStageError("exec", e.getClass().getSimpleName(), Map.of());
            throw e;
          }
        }
      }
      
      if (outputStr == null) {
        throw new ExecutionException("Plan execution failed after all retry attempts");
      }

      // 6. Update cache with execution status (plan was already cached after generation)
      // The plan itself doesn't change - it still has placeholders and JsonPath expressions
      // We're just updating the execution status metadata
      if (success) {
        // Update cache metadata to mark as successful (plan content unchanged)
        cache.store(schema, plan, true, null);
        log.info("Updated cache status to SUCCESS for PSK: {}", schema.getCacheKey());
      }

      log.trace("Generated answer:\n{}", StringUtility.formatWithIndent(outputStr, 4));
      StdoutUtility.printSuccessLine(
          oneMcp,
          "Assignment handled in (%s ms)\nAnswer: \n%s"
              .formatted(
                  (System.currentTimeMillis() - start),
                  StringUtility.formatWithIndent(outputStr, 4)));

      assignmentParts.add(
          new AssigmentResult.Assignment(
              true, prompt, false, outputStr));
      
      // Log final response
      oneMcp.inferenceLogger().logFinalResponse(outputStr);
      
    } catch (Exception e) {
      StdoutUtility.printError(oneMcp, "Could not handle assignment properly", e);
      // Use clean error message for user-facing display
      String errorMsg = ExceptionUtil.extractErrorMessage(e);
      assignmentParts.add(
          new AssigmentResult.Assignment(
              true, prompt, true, errorMsg));
      success = false;
      // Don't throw here - ensure report is generated first
    }

    // Build the result
    long totalTimeMs = System.currentTimeMillis() - start;
    AssigmentResult.Statistics stats =
        new AssigmentResult.Statistics(
            0, // promptTokens - TODO: track from plan context
            0, // completionTokens - TODO: track from plan context
            0, // totalTokens - TODO: track from plan context
            totalTimeMs,
            calledOperations,
            null); // trace - TODO: track from plan context
    
    AssigmentResult result = new AssigmentResult(assignmentParts, stats, null);
    
    // Log the assignment result for the report
    oneMcp.inferenceLogger().logAssigmentResult(result);
    
    // Complete execution tracking - ALWAYS generate report, even on errors
    oneMcp.inferenceLogger().completeExecution(executionId, totalTimeMs, success);
    
    // If there was an error, throw it after generating the report
    if (!success && !assignmentParts.isEmpty() && assignmentParts.get(assignmentParts.size() - 1).isError()) {
      String errorMsg = assignmentParts.get(assignmentParts.size() - 1).content();
      throw new ExecutionException(errorMsg);
    }
    
    return result;
  }

  /**
   * Load lexicon from cache directory, or generate it automatically if not available.
   * Lexicon is stored in ONEMCP_HOME_DIR/cache/lexicon.yaml
   */
  private PromptLexicon loadOrGenerateLexicon(Path handbookPath) {
    // Determine cache directory (same logic as ExecutionPlanCache)
    Path cacheDir;
    String cacheDirEnv = System.getenv("ONEMCP_CACHE_DIR");
    if (cacheDirEnv != null && !cacheDirEnv.isBlank()) {
      cacheDir = Paths.get(cacheDirEnv);
    } else {
      String homeDir = System.getenv("ONEMCP_HOME_DIR");
      if (homeDir != null && !homeDir.isBlank()) {
        cacheDir = Paths.get(homeDir, "cache");
      } else {
        String userHome = System.getProperty("user.home");
        if (userHome != null && !userHome.isBlank()) {
          cacheDir = Paths.get(userHome, ".onemcp", "cache");
        } else {
          // Final fallback: handbook/cache (should not happen in normal operation)
          cacheDir = handbookPath.resolve("cache");
        }
      }
    }
    
    Path lexiconPath = cacheDir.resolve("lexicon.yaml");
    
    // Try to load existing lexicon
    if (Files.exists(lexiconPath)) {
      try {
        LexiconExtractorService extractor = new LexiconExtractorService(oneMcp);
        PromptLexicon lexicon = extractor.loadLexicon(lexiconPath);
        if (lexicon != null) {
          log.info("Loaded lexicon from: {}", lexiconPath);
          return lexicon;
        }
      } catch (Exception e) {
        log.warn("Failed to load lexicon from {}: {}", lexiconPath, e.getMessage());
      }
    }
    
    // Lexicon doesn't exist - generate it automatically
    log.info("Lexicon not found at: {}. Generating automatically...", lexiconPath);
    try {
      // Ensure cache directory exists
      Files.createDirectories(cacheDir);
      
      // Generate lexicon using LexiconExtractorService
      LexiconExtractorService extractor = new LexiconExtractorService(oneMcp);
      PromptLexicon lexicon = extractor.extractLexicon(handbookPath);
      
      // Save to cache directory
      extractor.saveLexicon(lexicon, lexiconPath);
      log.info("Generated and saved lexicon to: {}", lexiconPath);
      
      return lexicon;
    } catch (Exception e) {
      log.error("Failed to generate lexicon: {}", e.getMessage(), e);
      // Re-throw with more context so the error message is clearer
      throw new HandbookException(
          "Failed to generate lexicon: " + e.getMessage() + 
          (e.getCause() != null ? " (Caused by: " + e.getCause().getMessage() + ")" : ""), e);
    }
  }
}
