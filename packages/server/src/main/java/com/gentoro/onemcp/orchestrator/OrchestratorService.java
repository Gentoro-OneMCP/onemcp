package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.messages.AssignmentContext;
import com.gentoro.onemcp.cache.Lexifier;
import com.gentoro.onemcp.cache.ConceptualLexicon;
import com.gentoro.onemcp.cache.PromptSchemaNormalizer;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.ValueConverter;
import com.gentoro.onemcp.cache.dag.DagPlan;
import com.gentoro.onemcp.cache.dag.DagPlanCache;
import com.gentoro.onemcp.cache.dag.DagPlanner;
import com.gentoro.onemcp.cache.dag.DagExecutor;
import com.gentoro.onemcp.orchestrator.progress.NoOpProgressSink;
import com.gentoro.onemcp.orchestrator.progress.ProgressSink;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
    // Default to false (caching disabled) - legacy path is the stable default
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
   *   <li>Legacy path: Uses orchestrator.PlanGenerationService (stable, default)
   *   <li>Caching path: Uses Planner + cache package (experimental, when CACHE_ENABLED=true)
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
   * This is the stable, production-ready path.
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

      // Prepare operations stage (we can't know execution count precisely here)
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
   * EXPERIMENTAL: Caching path using Prompt Schema (PS) normalization.
   * 
   * <p>This is an experimental feature that requires CACHE_ENABLED=true to activate.
   * 
   * <p>Flow: Prompt → Normalize (PS) → Canonicalize → Cache Key → Planner → TypeScript → Execute → Cache result
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
      
      // 1. Load conceptual lexicon
      progress.beginStage("lexicon", "Loading Conceptual Lexicon", 1);
      StdoutUtility.printNewLine(oneMcp, "Loading conceptual lexicon.");
      
      ConceptualLexicon lexicon;
      try {
        Lexifier lexifier = new Lexifier(oneMcp);
        lexicon = lexifier.loadLexicon();
      } catch (Exception e) {
        // Wrap other exceptions with cause message included
        String errorMsg = "Failed to load conceptual lexicon: " + e.getMessage();
        if (e.getCause() != null && e.getCause().getMessage() != null) {
          errorMsg += " (Caused by: " + e.getCause().getMessage() + ")";
        }
        throw new HandbookException(errorMsg + ". Run 'index' command first.", e);
      }
      if (lexicon == null || lexicon.getEntities().isEmpty()) {
        throw new HandbookException(
            "Lexicon is empty. Please run the 'index' command to generate the lexicon from your handbook.");
      }
      progress.endStageOk("lexicon", Map.of("entities", lexicon.getEntities().size(), 
          "fields", lexicon.getFields().size()));

      // 2. Normalize prompt to Prompt Schema (PS)
      progress.beginStage("normalize", "Normalizing prompt to PS", 1);
      StdoutUtility.printNewLine(oneMcp, "Normalizing prompt to PS.");
      
      PromptSchemaNormalizer normalizer = new PromptSchemaNormalizer(oneMcp);
      PromptSchema ps;
      try {
        ps = normalizer.normalize(prompt, lexicon);
      } catch (Exception e) {
        // Normalization threw an exception (e.g., JSON parsing error)
        String errorMsg = ExceptionUtil.extractErrorMessage(e);
        log.error("Normalization exception: {}", errorMsg, e);
        progress.endStageError("normalize", "NORMALIZATION_EXCEPTION", Map.of("error", errorMsg));
        
        // Log the error to execution plan so it appears in EXECUTION RESULT section
        try {
          Map<String, Object> errorPlanData = new HashMap<>();
          errorPlanData.put("operation", "normalization_failed");
          errorPlanData.put("error", errorMsg);
          errorPlanData.put("prompt", prompt);
          String errorPlanJson = JacksonUtility.getJsonMapper().writeValueAsString(errorPlanData);
          oneMcp.inferenceLogger().logExecutionPlanNew(errorPlanJson, null, errorMsg, false);
        } catch (Exception logErr) {
          log.warn("Failed to log normalization error to execution plan", logErr);
        }
        
        // Log the assignment result and complete execution BEFORE returning
        // This ensures the report is generated even when normalization fails early
        AssignmentContext context = new AssignmentContext();
        String assignmentError = "Normalization failed: " + errorMsg;
        context.setRefinedAssignment(assignmentError);
        AssigmentResult errorResult = new AssigmentResult(
            java.util.Collections.emptyList(),
            new AssigmentResult.Statistics(0, 0, 0, System.currentTimeMillis() - start, java.util.Collections.emptyList(), null),
            context);
        
        // Log assignment result and complete execution to generate report
        oneMcp.inferenceLogger().logAssigmentResult(errorResult);
        oneMcp.inferenceLogger().completeExecution(executionId, System.currentTimeMillis() - start, false);
        
        return errorResult;
      }
      
      // Check if normalization failed
      boolean normalizationFailed = ps.getOperation() == null || ps.getOperation().trim().isEmpty();
      String normalizationNote = ps.getNote();
      
      if (normalizationFailed) {
        String errorMsg = normalizationNote != null && !normalizationNote.trim().isEmpty()
            ? normalizationNote
            : "Normalization failed: Could not convert prompt to PS";
        log.warn("Normalization failed: {}", errorMsg);
        progress.endStageError("normalize", "NORMALIZATION_FAILED", Map.of("note", errorMsg));
        
        // Log assignment result and complete execution BEFORE returning
        // This ensures the report is generated even when normalization fails
        AssignmentContext context = new AssignmentContext();
        context.setRefinedAssignment(errorMsg);
        AssigmentResult errorResult = new AssigmentResult(
            java.util.Collections.emptyList(),
            new AssigmentResult.Statistics(0, 0, 0, System.currentTimeMillis() - start, java.util.Collections.emptyList(), null),
            context);
        
        // Log assignment result and complete execution to generate report
        oneMcp.inferenceLogger().logAssigmentResult(errorResult);
        oneMcp.inferenceLogger().completeExecution(executionId, System.currentTimeMillis() - start, false);
        
        return errorResult;
      }
      
      String cacheKey = ps.getCacheKey();
      log.info("Normalized prompt to PS: operation={}, table={}", ps.getOperation(), ps.getTable());
      if (normalizationNote != null && !normalizationNote.trim().isEmpty()) {
        log.info("Normalization note (caveat): {}", normalizationNote);
      }
      progress.endStageOk("normalize", Map.of("operation", ps.getOperation(), "table", ps.getTable(), "cacheKey", cacheKey));

      // 4. Check cache for existing plan (DAG or legacy TypeScript)
      String dagJson = null;
      boolean cacheHit = false;
      DagPlan dagPlan = null;
      
      // Use DAG system
      DagPlanCache dagPlanCache = new DagPlanCache(cacheDir);
      progress.beginStage("cache", "Checking DAG cache", 1);
      
      Optional<DagPlanCache.CachedDagPlanResult> cachedDagResult = 
          dagPlanCache.lookupByCacheKey(cacheKey);
      if (cachedDagResult.isPresent()) {
        cacheHit = true;
        dagPlan = cachedDagResult.get().getPlan();
        dagJson = dagPlan.toJson();
        log.info("DAG Cache HIT for cache key: {} (executionSucceeded: {}, failedAttempts: {})", 
            cacheKey, cachedDagResult.get().isExecutionSucceeded(), 
            cachedDagResult.get().getFailedAttempts());
      } else {
        log.info("DAG Cache MISS for cache key: {}", cacheKey);
      }
      progress.endStageOk("cache", Map.of("hit", cacheHit));
      
      log.debug("Cache lookup result: hit={}, dagJson={}", cacheHit, dagJson != null ? "present" : "null");

      // 5. Generate plan if cache miss
      if (dagJson == null) {
        // Generate DAG plan
        progress.beginStage("plan", "Generating DAG execution plan", 1);
        StdoutUtility.printNewLine(oneMcp, "Generating DAG execution plan from PS.");
        
        DagPlanner dagPlanner = new DagPlanner(oneMcp, endpointInfoDir);
        dagJson = dagPlanner.planDag(ps, prompt);
        dagPlan = DagPlan.fromJsonString(dagJson);
        log.info("Generated DAG plan with {} nodes", dagPlan.getNodes().size());
        
        progress.endStageOk("plan", Map.of("nodes", dagPlan.getNodes().size()));
        
        // Cache the DAG plan
        DagPlanCache dagPlanCacheForStore = new DagPlanCache(cacheDir);
        dagPlanCacheForStore.storeByCacheKey(cacheKey, dagPlan, false, null);
        log.debug("Cached newly generated DAG plan for cache key: {}", cacheKey);
      }

      // Log DAG plan for reporting (whether from cache or newly generated)
      // CRITICAL: Always log the plan so it appears in the report
      // If no plan but there's a note, return with just the note
      if (dagJson == null || dagJson.trim().isEmpty()) {
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
          // No plan and no note - this shouldn't happen if normalization succeeded
          log.error("Cannot log execution plan: dagJson is null or empty and no note");
          throw new ExecutionException("DAG plan is null - cannot proceed");
        }
      }
      
      try {
        // Include PS (Prompt Schema) information in the plan JSON
        Map<String, Object> planData = new HashMap<>();
        planData.put("dag", dagJson);
        planData.put("operation", ps.getOperation());
        planData.put("table", ps.getTable());
        planData.put("fields", ps.getFields());
        planData.put("where", ps.getWhere());
        planData.put("group_by", ps.getGroupBy());
        planData.put("order_by", ps.getOrderBy());
        planData.put("limit", ps.getLimit());
        planData.put("offset", ps.getOffset());
        planData.put("values", ps.getValues() != null ? ps.getValues() : java.util.Collections.emptyList());
        planData.put("columns", ps.getColumns() != null ? ps.getColumns() : java.util.Collections.emptyList());
        planData.put("cache_key", cacheKey);
        if (normalizationNote != null && !normalizationNote.trim().isEmpty()) {
          planData.put("note", normalizationNote);
        }
        
        String planJson = JacksonUtility.getJsonMapper().writeValueAsString(planData);
        // Use logExecutionPlanNew to ensure it's a separate event (will be updated with result/error later)
        oneMcp.inferenceLogger().logExecutionPlanNew(planJson, null, null, cacheHit);
        log.debug("Logged DAG plan with PS info (cache hit: {}, length: {})", 
            cacheHit, dagJson.length());
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

      // 6. Execute DAG plan
      String outputStr = null;
      progress.beginStage("exec", "Executing DAG plan", 1);
      StdoutUtility.printNewLine(oneMcp, "Executing DAG plan.");
      
      // Build operation registry for DAG
      OperationRegistry operationRegistry = new OperationRegistry();
      
      // Register API operations
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
      
      if (dagPlan == null) {
        dagPlan = DagPlan.fromJsonString(dagJson);
      }
      
      // Convert PS.values to Map<String, String> for initial values
      Map<String, String> psValues = new HashMap<>();
      if (ps.getValues() != null) {
        for (Map.Entry<String, Object> entry : ps.getValues().entrySet()) {
          psValues.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
      }
      
      // Convert conceptual values to API format before execution
      // This analyzes ConvertValue nodes in the DAG and converts PS values upfront
      com.gentoro.onemcp.cache.dag.ValueConversionService conversionService = 
          new com.gentoro.onemcp.cache.dag.ValueConversionService();
      Map<String, String> initialValues = conversionService.convertValues(dagPlan, psValues);
      
      // Execute using DagExecutor
      DagExecutor dagExecutor = new DagExecutor(operationRegistry);
      
      JsonNode result = dagExecutor.execute(dagPlan, initialValues);
      outputStr = JacksonUtility.getJsonMapper().writeValueAsString(result);
      
      if (outputStr == null || outputStr.trim().isEmpty()) {
        throw new ExecutionException("DAG execution returned null or empty result");
      }
      
      success = true;
      
      // Cache success
      DagPlanCache dagPlanCacheForSuccess = new DagPlanCache(cacheDir);
      dagPlanCacheForSuccess.storeByCacheKey(cacheKey, dagPlan, true, null);
      
      progress.endStageOk("exec", Map.of("result", "success"));
      
      // Log execution result
      try {
        Map<String, Object> planData = new HashMap<>();
        planData.put("dag", dagJson);
        planData.put("operation", ps.getOperation());
        planData.put("table", ps.getTable());
        planData.put("values", ps.getValues() != null ? ps.getValues() : new HashMap<>());
        planData.put("cache_key", cacheKey);
        String planJson = JacksonUtility.getJsonMapper().writeValueAsString(planData);
        oneMcp.inferenceLogger().logExecutionPlanNew(planJson, outputStr, null, cacheHit);
      } catch (Exception logErr) {
        log.warn("Failed to log plan execution result", logErr);
      }
      
      // Continue to shared response handling below
      // (outputStr and success are set)
      
      if (outputStr == null) {
        throw new ExecutionException("Plan execution failed");
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
   * Index the conceptual lexicon by extracting it from the API specification.
   * This uses the Lexifier which produces a flat vocabulary of actions, entities, and fields.
   * 
   * @return the report path if available, or null
   * @throws HandbookException if lexicon extraction fails
   */
  public String indexSchema() throws HandbookException {
    Path handbookPath = oneMcp.handbook().location();
    if (handbookPath == null) {
      throw new HandbookException("Handbook location is not available");
    }

    Path cacheDir = getCacheDirectory(handbookPath);
    
    // Force regeneration by deleting the lexicon file if it exists
    Path lexiconPath = cacheDir.resolve("lexicon.json");
    try {
      if (Files.exists(lexiconPath)) {
        Files.delete(lexiconPath);
        log.info("Deleted existing lexicon file to force regeneration: {}", lexiconPath);
      }
    } catch (IOException e) {
      log.warn("Failed to delete existing lexicon file: {}", e.getMessage());
    }
    
    // Generate lexicon using Lexifier
    try {
      // Ensure cache directory exists
      Files.createDirectories(cacheDir);
      
      // Generate lexicon using Lexifier
      Lexifier lexifier = new Lexifier(oneMcp);
      ConceptualLexicon lexicon = lexifier.extractLexicon();
      
      log.info("Successfully indexed conceptual lexicon: {} actions, {} entities, {} fields", 
          lexicon.getActions().size(), lexicon.getEntities().size(), lexicon.getFields().size());
      
      // Get the lexifier report path
      return oneMcp.inferenceLogger().getCurrentReportPath();
    } catch (Exception e) {
      log.error("Failed to index conceptual lexicon: {}", e.getMessage(), e);
      throw new HandbookException(
          "Failed to index conceptual lexicon: " + e.getMessage() + 
          (e.getCause() != null ? " (Caused by: " + e.getCause().getMessage() + ")" : ""), e);
    }
  }

  /**
   * Register format conversion operations for DAG execution.
   * Includes a generic script execution operation for arbitrary conversions.
   */
  private void registerFormatConversionOperations(OperationRegistry registry, Path cacheDir) {
    // Generic script execution operation - allows LLM to generate arbitrary conversion code
    registry.register("executeScript", (input) -> {
      try {
        String code = input.has("code") ? input.get("code").asText() : null;
        JsonNode inputData = input.has("input") ? input.get("input") : JacksonUtility.getJsonMapper().createObjectNode();
        
        if (code == null || code.trim().isEmpty()) {
          throw new ExecutionPlanException("executeScript: missing 'code' parameter");
        }
        
        // Execute JavaScript code using Node.js
        // Input is passed as JSON, code should return the converted value
        Path tempDir = cacheDir.resolve("ts-temp");
        Files.createDirectories(tempDir);
        
        String uuid = UUID.randomUUID().toString();
        Path jsFile = tempDir.resolve("script_" + uuid + ".js");
        Path resultFile = tempDir.resolve("script_result_" + uuid + ".json");
        
        // Create JavaScript file that executes the conversion code
        String inputJson = JacksonUtility.getJsonMapper().writeValueAsString(inputData);
        String jsCode = String.format("""
            const input = %s;
            const fs = require('fs');
            let result;
            try {
              %s
              fs.writeFileSync('%s', JSON.stringify({ result: result }));
            } catch (error) {
              fs.writeFileSync('%s', JSON.stringify({ error: error.message }));
              process.exit(1);
            }
            """, inputJson, code, resultFile.toString(), resultFile.toString());
        
        Files.writeString(jsFile, jsCode);
        
        // Execute using Node.js
        ProcessBuilder pb = new ProcessBuilder("node", jsFile.toString());
        pb.directory(tempDir.toFile());
        Process process = pb.start();
        
        boolean finished = process.waitFor(5, TimeUnit.SECONDS);
        if (!finished) {
          process.destroyForcibly();
          throw new ExecutionPlanException("Script execution timed out");
        }
        
        if (process.exitValue() != 0) {
          // Try to read error from result file
          if (Files.exists(resultFile)) {
            String errorContent = Files.readString(resultFile);
            JsonNode errorJson = JacksonUtility.getJsonMapper().readTree(errorContent);
            if (errorJson.has("error")) {
              throw new ExecutionPlanException("Script execution failed: " + errorJson.get("error").asText());
            }
          }
          throw new ExecutionPlanException("Script execution failed with exit code " + process.exitValue());
        }
        
        // Read result
        if (!Files.exists(resultFile)) {
          throw new ExecutionPlanException("Script did not produce result file");
        }
        
        String resultContent = Files.readString(resultFile);
        JsonNode resultJson = JacksonUtility.getJsonMapper().readTree(resultContent);
        
        if (resultJson.has("error")) {
          throw new ExecutionPlanException("Script execution error: " + resultJson.get("error").asText());
        }
        
        // Cleanup
        try {
          Files.deleteIfExists(jsFile);
          Files.deleteIfExists(resultFile);
        } catch (Exception e) {
          log.debug("Failed to cleanup script files", e);
        }
        
        return resultJson;
      } catch (Exception e) {
        throw new ExecutionPlanException("executeScript failed: " + e.getMessage(), e);
      }
    });
    
    // Keep the pre-defined conversions for common cases (optional optimization)
    // Convert ISO 8601 datetime to 12-hour time format (e.g., "2:30 PM")
    registry.register("convertISO8601To12Hour", (input) -> {
      try {
        String iso = input.has("iso") ? input.get("iso").asText() : null;
        if (iso == null || iso.isEmpty()) {
          throw new ExecutionPlanException("convertISO8601To12Hour: missing 'iso' parameter");
        }
        java.time.Instant instant = java.time.Instant.parse(iso);
        java.time.LocalTime time = instant.atZone(java.time.ZoneId.systemDefault()).toLocalTime();
        java.time.format.DateTimeFormatter formatter = 
            java.time.format.DateTimeFormatter.ofPattern("h:mm a");
        String result = time.format(formatter);
        return JacksonUtility.getJsonMapper().createObjectNode().put("result", result);
      } catch (Exception e) {
        throw new ExecutionPlanException("convertISO8601To12Hour failed: " + e.getMessage(), e);
      }
    });

    // Convert ISO 8601 datetime to 24-hour time format (e.g., "14:30")
    registry.register("convertISO8601To24Hour", (input) -> {
      try {
        String iso = input.has("iso") ? input.get("iso").asText() : null;
        if (iso == null || iso.isEmpty()) {
          throw new ExecutionPlanException("convertISO8601To24Hour: missing 'iso' parameter");
        }
        java.time.Instant instant = java.time.Instant.parse(iso);
        java.time.LocalTime time = instant.atZone(java.time.ZoneId.systemDefault()).toLocalTime();
        String result = time.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm"));
        return JacksonUtility.getJsonMapper().createObjectNode().put("result", result);
      } catch (Exception e) {
        throw new ExecutionPlanException("convertISO8601To24Hour failed: " + e.getMessage(), e);
      }
    });

    // Convert ISO 8601 datetime to date only (e.g., "2024-12-06")
    registry.register("convertISO8601ToDate", (input) -> {
      try {
        String iso = input.has("iso") ? input.get("iso").asText() : null;
        if (iso == null || iso.isEmpty()) {
          throw new ExecutionPlanException("convertISO8601ToDate: missing 'iso' parameter");
        }
        java.time.Instant instant = java.time.Instant.parse(iso);
        java.time.LocalDate date = instant.atZone(java.time.ZoneId.systemDefault()).toLocalDate();
        String result = date.toString();
        return JacksonUtility.getJsonMapper().createObjectNode().put("result", result);
      } catch (Exception e) {
        throw new ExecutionPlanException("convertISO8601ToDate failed: " + e.getMessage(), e);
      }
    });

    // Convert string number to number type
    registry.register("convertStringToNumber", (input) -> {
      try {
        String str = input.has("value") ? input.get("value").asText() : null;
        if (str == null || str.isEmpty()) {
          throw new ExecutionPlanException("convertStringToNumber: missing 'value' parameter");
        }
        double num = Double.parseDouble(str);
        return JacksonUtility.getJsonMapper().createObjectNode().put("result", num);
      } catch (Exception e) {
        throw new ExecutionPlanException("convertStringToNumber failed: " + e.getMessage(), e);
      }
    });

    // Convert string boolean to boolean type
    registry.register("convertStringToBoolean", (input) -> {
      try {
        String str = input.has("value") ? input.get("value").asText() : null;
        if (str == null || str.isEmpty()) {
          throw new ExecutionPlanException("convertStringToBoolean: missing 'value' parameter");
        }
        boolean bool = "true".equalsIgnoreCase(str);
        return JacksonUtility.getJsonMapper().createObjectNode().put("result", bool);
      } catch (Exception e) {
        throw new ExecutionPlanException("convertStringToBoolean failed: " + e.getMessage(), e);
      }
    });
  }

}
