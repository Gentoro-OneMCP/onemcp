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
import com.gentoro.onemcp.cache.ConceptualSchema;
import com.gentoro.onemcp.cache.TransducerService;
import com.gentoro.onemcp.cache.SsqlNormalizer;
import com.gentoro.onemcp.cache.SsqlCanonicalizer;
import com.gentoro.onemcp.cache.Planner;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.TypeScriptExecutionEngine;
import com.gentoro.onemcp.cache.TypeScriptPlanCache;
import com.gentoro.onemcp.orchestrator.progress.NoOpProgressSink;
import com.gentoro.onemcp.orchestrator.progress.ProgressSink;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
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
    // The config should already have the env var interpolated: ${env:CACHE_ENABLED:-true}
    // Default to true (caching enabled) since that's the new recommended path
    boolean configValue = oneMcp.configuration().getBoolean("orchestrator.cache.enabled", true);
    
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
   *   <li>Caching path: Uses Planner + cache package (when orchestrator.cache.enabled=true)
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

      // Log execution plan for reporting (will be updated with cache status later)
      // Don't log here - wait until we know the cache status

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
   * Caching path: Uses S-SQL-based caching (cache_spec v10.0).
   * 
   * <p>Flow: Prompt → Normalize (S-SQL) → Canonicalize → Cache Key → Planner → TypeScript → ExecutionPlan → Execute → Cache result
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
      // Determine cache directories
      Path cacheDir = getCacheDirectory(handbookPath);
      Path endpointInfoDir = cacheDir.resolve("endpoint_info");
      
      // 1. Load or generate conceptual schema
      progress.beginStage("transducer", "Loading/Generating Conceptual Schema", 1);
      StdoutUtility.printNewLine(oneMcp, "Loading/Generating conceptual schema.");
      
      ConceptualSchema schema;
      try {
        schema = loadOrGenerateConceptualSchema(handbookPath, endpointInfoDir);
      } catch (HandbookException e) {
        // Re-throw HandbookException as-is (already has proper error message)
        throw e;
      } catch (Exception e) {
        // Wrap other exceptions with cause message included
        String errorMsg = "Failed to load or generate conceptual schema: " + e.getMessage();
        if (e.getCause() != null && e.getCause().getMessage() != null) {
          errorMsg += " (Caused by: " + e.getCause().getMessage() + ")";
        }
        throw new HandbookException(errorMsg, e);
      }
      if (schema == null || schema.getTables().isEmpty()) {
        throw new HandbookException(
            "Failed to load or generate conceptual schema. Please check logs for details.");
      }
      progress.endStageOk("transducer", Map.of("tables", schema.getTables().size()));

      // 2. Normalize prompt to S-SQL and produce Prompt Schema (PS)
      progress.beginStage("normalize", "Normalizing prompt to S-SQL", 1);
      StdoutUtility.printNewLine(oneMcp, "Normalizing prompt to S-SQL.");
      
      SsqlNormalizer normalizer = new SsqlNormalizer(oneMcp);
      PromptSchema ps = normalizer.normalize(prompt, schema);
      
      // Check if normalization failed
      boolean normalizationFailed = ps.getSsql() == null || ps.getSsql().trim().isEmpty();
      String normalizationNote = ps.getNote();
      
      if (normalizationFailed) {
        String errorMsg = normalizationNote != null && !normalizationNote.trim().isEmpty()
            ? normalizationNote
            : "Normalization failed: Could not convert prompt to S-SQL";
        log.warn("Normalization failed: {}", errorMsg);
        progress.endStageError("normalize", "NORMALIZATION_FAILED", Map.of("note", errorMsg));
        // Return the explanation as the result (normalization failures are cached)
        AssignmentContext context = new AssignmentContext();
        context.setRefinedAssignment(errorMsg);
        return new AssigmentResult(
            java.util.Collections.emptyList(),
            new AssigmentResult.Statistics(0, 0, 0, 0, java.util.Collections.emptyList(), null),
            context);
      }
      
      log.info("Normalized prompt to S-SQL: {}", ps.getSsql());
      if (normalizationNote != null && !normalizationNote.trim().isEmpty()) {
        log.info("Normalization note (caveat): {}", normalizationNote);
      }
      progress.endStageOk("normalize", Map.of("ssql", ps.getSsql()));

      // 3. Canonicalize S-SQL and generate cache key
      progress.beginStage("canonicalize", "Canonicalizing S-SQL", 1);
      StdoutUtility.printNewLine(oneMcp, "Canonicalizing S-SQL.");
      
      SsqlCanonicalizer canonicalizer = new SsqlCanonicalizer();
      String canonicalSsql = canonicalizer.canonicalize(ps.getSsql());
      // Cache key is based ONLY on canonical S-SQL, not parameter values
      // The TypeScript plan is parameterized and uses values at runtime
      // Different parameter values (e.g., 2024 vs 2023) should use the same cached plan
      String cacheKey = canonicalizer.generateCacheKey(canonicalSsql);
      log.info("Canonical S-SQL: {}", canonicalSsql);
      log.info("S-SQL-based cache key: {}", cacheKey);
      progress.endStageOk("canonicalize", Map.of("cacheKey", cacheKey));

      // 4. Check cache for existing TypeScript plan
      String typescriptCode = null;
      boolean cacheHit = false;
      Optional<TypeScriptPlanCache.CachedPlanResult> cachedResult = Optional.empty();
      TypeScriptPlanCache planCache = new TypeScriptPlanCache(cacheDir);
      
      progress.beginStage("cache", "Checking cache", 1);
      
      cachedResult = planCache.lookupByCacheKey(cacheKey);
      if (cachedResult.isPresent()) {
        cacheHit = true;
        typescriptCode = cachedResult.get().typescriptCode;
        // Use normalization metadata from cache if available, otherwise use from PS
        if (cachedResult.get().normalizationNote != null) {
          normalizationNote = cachedResult.get().normalizationNote;
        }
        if (cachedResult.get().normalizationFailed) {
          normalizationFailed = true;
          // If normalization failed, return the explanation immediately
          String errorMsg = normalizationNote != null && !normalizationNote.trim().isEmpty()
              ? normalizationNote
              : "Normalization failed: Could not convert prompt to S-SQL";
          log.warn("Cached plan has normalization failure: {}", errorMsg);
          AssignmentContext context = new AssignmentContext();
          context.setRefinedAssignment(errorMsg);
          return new AssigmentResult(
              java.util.Collections.emptyList(),
              new AssigmentResult.Statistics(0, 0, 0, 0, java.util.Collections.emptyList(), null),
              context);
        }
        // Check if cached plan has no code but has a note (shouldn't happen, but handle gracefully)
        if ((typescriptCode == null || typescriptCode.trim().isEmpty()) && 
            normalizationNote != null && !normalizationNote.trim().isEmpty()) {
          log.info("Cached plan has no code but has note: {}", normalizationNote);
          assignmentParts.add(
              new AssigmentResult.Assignment(
                  true, prompt, false, normalizationNote));
          oneMcp.inferenceLogger().logFinalResponse(normalizationNote);
          
          long totalTimeMs = System.currentTimeMillis() - start;
          AssigmentResult.Statistics stats =
              new AssigmentResult.Statistics(0, 0, 0, totalTimeMs, calledOperations, null);
          AssigmentResult result = new AssigmentResult(assignmentParts, stats, null);
          oneMcp.inferenceLogger().logAssigmentResult(result);
          oneMcp.inferenceLogger().completeExecution(executionId, totalTimeMs, true);
          return result;
        }
        log.info("Cache HIT for S-SQL cache key: {} (executionSucceeded: {}, failedAttempts: {})", 
            cacheKey, cachedResult.get().executionSucceeded, cachedResult.get().failedAttempts);
        if (!cachedResult.get().executionSucceeded && cachedResult.get().failedAttempts > 0) {
          log.warn("Cached plan previously failed ({} attempts). Will retry execution.",
              cachedResult.get().failedAttempts);
        }
      } else {
        log.info("Cache MISS for S-SQL cache key: {}", cacheKey);
      }
      progress.endStageOk("cache", Map.of("hit", cacheHit));
      log.debug("Cache lookup result: hit={}, typescriptCode={}", cacheHit, typescriptCode != null ? "present" : "null");

      // 5. Generate TypeScript plan if cache miss
      if (typescriptCode == null) {
        progress.beginStage("plan", "Generating execution plan", 1);
        StdoutUtility.printNewLine(oneMcp, "Generating execution plan from S-SQL.");
        
        Planner planner = new Planner(oneMcp, endpointInfoDir);
        typescriptCode = planner.plan(ps, prompt);
        log.info("Generated TypeScript plan (length: {})", typescriptCode.length());
        
        progress.endStageOk("plan", Map.of("steps", "generated"));
        
        // Cache the TypeScript plan IMMEDIATELY after generation, before execution
        // Include normalization metadata (note and failed status)
        planCache.storeByCacheKey(cacheKey, typescriptCode, false, null, normalizationNote, normalizationFailed);
        log.debug("Cached newly generated TypeScript plan for S-SQL cache key: {}", cacheKey);
      }

      // Log TypeScript plan for reporting (whether from cache or newly generated)
      // CRITICAL: Always log the plan so it appears in the report
      // If no code but there's a note, return with just the note
      if (typescriptCode == null || typescriptCode.trim().isEmpty()) {
        if (normalizationNote != null && !normalizationNote.trim().isEmpty()) {
          // Plan has no code but has a note - return with just the note
          log.info("Plan has no code but has note: {}", normalizationNote);
          assignmentParts.add(
              new AssigmentResult.Assignment(
                  true, prompt, false, normalizationNote));
          oneMcp.inferenceLogger().logFinalResponse(normalizationNote);
          
          // Build the result
          long totalTimeMs = System.currentTimeMillis() - start;
          AssigmentResult.Statistics stats =
              new AssigmentResult.Statistics(0, 0, 0, totalTimeMs, calledOperations, null);
          AssigmentResult result = new AssigmentResult(assignmentParts, stats, null);
          oneMcp.inferenceLogger().logAssigmentResult(result);
          oneMcp.inferenceLogger().completeExecution(executionId, totalTimeMs, true);
          return result;
        } else {
          // No code and no note - this shouldn't happen if normalization succeeded
          log.error("Cannot log execution plan: typescriptCode is null or empty and no note");
          throw new ExecutionException("TypeScript plan is null - cannot proceed");
        }
      }
      
      try {
        // Include PS (Prompt Schema) information in the plan JSON
        Map<String, Object> planData = new HashMap<>();
        planData.put("typescript", typescriptCode);
        planData.put("ssql", ps.getSsql());
        planData.put("table", ps.getTable());
        planData.put("values", ps.getValues() != null ? ps.getValues() : java.util.Collections.emptyMap());
        planData.put("columns", ps.getColumns() != null ? ps.getColumns() : java.util.Collections.emptyList());
        planData.put("cache_key", cacheKey);
        if (normalizationNote != null && !normalizationNote.trim().isEmpty()) {
          planData.put("note", normalizationNote);
        }
        
        String planJson = JacksonUtility.getJsonMapper().writeValueAsString(planData);
        // Use logExecutionPlanNew to ensure it's a separate event (will be updated with result/error later)
        oneMcp.inferenceLogger().logExecutionPlanNew(planJson, null, null, cacheHit);
        log.debug("Logged TypeScript plan with PS info (cache hit: {}, length: {})", 
            cacheHit, typescriptCode.length());
      } catch (Exception e) {
        log.error("Failed to log execution plan", e);
        // Try to log an error message as the plan
        try {
          oneMcp.inferenceLogger().logExecutionPlanNew(
              "{\"error\": \"Failed to serialize plan: " + e.getMessage() + "\"}", null, null, null);
        } catch (Exception e2) {
          log.error("Failed to log error plan", e2);
        }
        // Continue execution even if logging fails
      }

      // 6. Execute TypeScript plan
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

      // Execute TypeScript directly using TypeScriptExecutionEngine
      TypeScriptExecutionEngine tsEngine = new TypeScriptExecutionEngine(operationRegistry, cacheDir.resolve("ts-temp"), oneMcp);
      
      String outputStr = null;
      boolean executionRetried = false;
      int maxExecutionRetries = 1; // Only one attempt unless there's an actual error
      
      for (int execAttempt = 1; execAttempt <= maxExecutionRetries; execAttempt++) {
        long execStartTime = System.currentTimeMillis();
        try {
          outputStr = tsEngine.execute(typescriptCode, operationRegistry, ps.getValues());
          long execDuration = System.currentTimeMillis() - execStartTime;
          
          // Log execution phase with duration
          oneMcp.inferenceLogger().logExecutionPhase(execAttempt, execDuration, true, null);
          if (outputStr == null || outputStr.trim().isEmpty()) {
            log.error("Plan execution returned null or empty result (attempt {}, cache hit: {})", 
                execAttempt, cacheHit && execAttempt == 1);
            throw new ExecutionException("Plan execution returned null or empty result");
          }
          success = true;
          progress.endStageOk("exec", Map.of("result", "success"));
          
          // Log execution result with the current plan (create new event, don't update existing)
          try {
            // Include PS information in the plan JSON
            Map<String, Object> planData = new HashMap<>();
            planData.put("typescript", typescriptCode);
            planData.put("ssql", ps.getSsql());
            planData.put("table", ps.getTable());
            planData.put("values", ps.getValues() != null ? ps.getValues() : java.util.Collections.emptyMap());
            planData.put("columns", ps.getColumns() != null ? ps.getColumns() : java.util.Collections.emptyList());
            planData.put("cache_key", cacheKey);
            
            String planJson = JacksonUtility.getJsonMapper().writeValueAsString(planData);
            if (planJson != null && !planJson.trim().isEmpty()) {
              // Always create new event for each attempt (don't update existing)
              oneMcp.inferenceLogger().logExecutionPlanNew(planJson, outputStr, null, cacheHit && execAttempt == 1);
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
          log.error("Plan execution failed (attempt {}/{}, cache hit: {})", 
              execAttempt, maxExecutionRetries, cacheHit && execAttempt == 1, e);
          
          // Log execution phase with error
          long execDuration = System.currentTimeMillis() - execStartTime;
          oneMcp.inferenceLogger().logExecutionPhase(execAttempt, execDuration, false, errorMsg);
          
          // Log execution error with the current plan (use clean error message, not stack trace)
          try {
            // Include PS information in the plan JSON
            Map<String, Object> planData = new HashMap<>();
            planData.put("typescript", typescriptCode);
            planData.put("ssql", ps.getSsql());
            planData.put("table", ps.getTable());
            planData.put("values", ps.getValues() != null ? ps.getValues() : java.util.Collections.emptyMap());
            planData.put("columns", ps.getColumns() != null ? ps.getColumns() : java.util.Collections.emptyList());
            planData.put("cache_key", cacheKey);
            
            String planJson = JacksonUtility.getJsonMapper().writeValueAsString(planData);
            if (planJson != null && !planJson.trim().isEmpty()) {
              // Always create new event for each attempt (don't update existing)
              oneMcp.inferenceLogger().logExecutionPlanNew(planJson, null, errorMsg, cacheHit && execAttempt == 1);
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
            executionRetried = true;
            
            // Regenerate TypeScript plan
            progress.beginStage("plan", "Regenerating plan after API failure", 1);
            Planner planner = new Planner(oneMcp, endpointInfoDir);
            typescriptCode = planner.plan(ps, prompt);
            
            cacheHit = false; // Next attempt uses new plan
            log.info("Regenerated S-SQL-based TypeScript plan (retry attempt {})", execAttempt + 1);
            progress.endStageOk("plan", Map.of("steps", "regenerated", "retry", execAttempt + 1));
            
            // Re-log the new plan (without result yet - will be logged after execution)
            try {
              // Include PS information in the plan JSON
              Map<String, Object> planData = new HashMap<>();
              planData.put("typescript", typescriptCode);
              planData.put("ssql", ps.getSsql());
              planData.put("table", ps.getTable());
              planData.put("values", ps.getValues() != null ? ps.getValues() : java.util.Collections.emptyMap());
              planData.put("columns", ps.getColumns() != null ? ps.getColumns() : java.util.Collections.emptyList());
              planData.put("cache_key", cacheKey);
              
              String planJson = JacksonUtility.getJsonMapper().writeValueAsString(planData);
              if (planJson != null && !planJson.trim().isEmpty()) {
                oneMcp.inferenceLogger().logExecutionPlanNew(planJson, null, null, false); // Regenerated plan is always a cache miss
              }
            } catch (Exception logErr) {
              log.warn("Failed to log regenerated plan", logErr);
            }
            
            progress.endStageError("exec", e.getClass().getSimpleName(), Map.of("retrying", true));
            continue; // Retry with new plan
          } else {
            // Last attempt failed or non-API error - cache failure and throw
            success = false;
            planCache.storeByCacheKey(cacheKey, typescriptCode, false, errorMsg, normalizationNote, normalizationFailed);
            progress.endStageError("exec", e.getClass().getSimpleName(), Map.of());
            throw e;
          }
        }
      }
      
      if (outputStr == null) {
        throw new ExecutionException("Plan execution failed after all retry attempts");
      }

      // 7. Update cache with execution status (plan was already cached during generation)
      if (success) {
        planCache.storeByCacheKey(cacheKey, typescriptCode, true, null, normalizationNote, normalizationFailed);
        log.info("Updated cache status to SUCCESS for S-SQL cache key: {}", cacheKey);
      }

      log.trace("Generated answer:\n{}", StringUtility.formatWithIndent(outputStr, 4));
      StdoutUtility.printSuccessLine(
          oneMcp,
          "Assignment handled in (%s ms)\nAnswer: \n%s"
              .formatted(
                  (System.currentTimeMillis() - start),
                  StringUtility.formatWithIndent(outputStr, 4)));

      // Attach note to result if present (caveat)
      String finalOutput = outputStr;
      if (normalizationNote != null && !normalizationNote.trim().isEmpty()) {
        // Append note as a caveat to the result
        finalOutput = outputStr + "\n\nNote: " + normalizationNote;
        log.info("Attached normalization note (caveat) to result");
      }

      assignmentParts.add(
          new AssigmentResult.Assignment(
              true, prompt, false, finalOutput));
      
      // Log final response (with note if present)
      oneMcp.inferenceLogger().logFinalResponse(finalOutput);
      
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
   * Get cache directory path.
   */
  private Path getCacheDirectory(Path handbookPath) {
    String cacheDirEnv = System.getenv("ONEMCP_CACHE_DIR");
    if (cacheDirEnv != null && !cacheDirEnv.isBlank()) {
      return Paths.get(cacheDirEnv);
    }
    String homeDir = System.getenv("ONEMCP_HOME_DIR");
    if (homeDir != null && !homeDir.isBlank()) {
      return Paths.get(homeDir, "cache");
    }
    String userHome = System.getProperty("user.home");
    if (userHome != null && !userHome.isBlank()) {
      return Paths.get(userHome, ".onemcp", "cache");
    }
    // Final fallback: handbook/cache
    return handbookPath.resolve("cache");
  }

  /**
   * Load schema from cache directory, or generate it automatically if not available.
   * Schema is stored in ONEMCP_HOME_DIR/cache/conceptual_schema.yaml
   * Endpoint indexes are stored in ONEMCP_HOME_DIR/cache/endpoint_info/<table_name>.json
   */
  private ConceptualSchema loadOrGenerateConceptualSchema(Path handbookPath, Path endpointInfoDir) {
    Path cacheDir = getCacheDirectory(handbookPath);
    Path schemaPath = cacheDir.resolve("conceptual_schema.yaml");
    
    // Try to load existing conceptual schema
    if (Files.exists(schemaPath)) {
      try {
        ConceptualSchema schema = JacksonUtility.getYamlMapper()
            .readValue(schemaPath.toFile(), ConceptualSchema.class);
        if (schema != null && !schema.getTables().isEmpty()) {
          log.info("Loaded conceptual schema from: {}", schemaPath);
          return schema;
        }
      } catch (Exception e) {
        log.warn("Failed to load conceptual schema from {}: {}", schemaPath, e.getMessage());
      }
    }
    
    // Schema doesn't exist - generate it automatically using transducer
    log.info("Schema not found at: {}. Generating automatically using transducer (v12.0)...", schemaPath);
    try {
      // Ensure cache directories exist
      Files.createDirectories(cacheDir);
      Files.createDirectories(endpointInfoDir);
      
      // Generate schema using TransducerService (cache_spec v12.0)
      // This implements single-pass extraction with 4 phases
      TransducerService transducer = new TransducerService(oneMcp, cacheDir);
      ConceptualSchema schema = transducer.extractConceptualSchema(handbookPath, endpointInfoDir);
      
      log.info("Successfully generated conceptual schema: {} tables", schema.getTables().size());
      
      return schema;
    } catch (Exception e) {
      log.error("Failed to generate conceptual schema: {}", e.getMessage(), e);
      throw new HandbookException(
          "Failed to generate conceptual schema: " + e.getMessage() + 
          (e.getCause() != null ? " (Caused by: " + e.getCause().getMessage() + ")" : ""), e);
    }
  }

}
