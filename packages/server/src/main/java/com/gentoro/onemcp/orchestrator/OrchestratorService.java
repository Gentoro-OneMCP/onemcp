package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.cache.ConceptualLexicon;
import com.gentoro.onemcp.cache.ExecutionPlanCache;
import com.gentoro.onemcp.cache.Lexifier;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.PromptSchemaNormalizer;
import com.gentoro.onemcp.cache.dag.DagPlanner;
import com.gentoro.onemcp.plan.ExecutionPlan;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.openapi.OpenApiProxy;
import com.gentoro.onemcp.openapi.OpenApiProxyImpl;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

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
    String reportPath = oneMcp.inferenceLogger().startExecution(executionId, prompt);

    OrchestratorContext ctx = new OrchestratorContext(oneMcp, new ValueStore());
    long start = System.currentTimeMillis();
    
    String result = null;
    boolean success = false;
    
    try {
      // ═══════════════════════════════════════════════════════════════════════
      // NEW FLOW: Normalize → Cache Lookup → (Generate if miss) → Execute
      // ═══════════════════════════════════════════════════════════════════════
      
      Path handbookPath = getHandbookPath(ctx);
      if (handbookPath == null) {
        throw new ExecutionException("No handbook path available. Set HANDBOOK_DIR or configure a handbook.");
      }
      
      // Load lexicon (auto-generate if missing)
      Lexifier lexifier = new Lexifier(oneMcp);
      ConceptualLexicon lexicon = null;
      
      try {
        lexicon = lexifier.loadLexicon();
      } catch (Exception e) {
        // Lexicon not found - auto-generate it
        log.info("Lexicon not found. Auto-generating from handbook...", e);
        StdoutUtility.printNewLine(oneMcp, "Generating lexicon from handbook...");
        
        try {
          lexicon = lexifier.extractLexicon();
          log.info("Lexicon auto-generated");
          StdoutUtility.printNewLine(oneMcp, 
              String.format("Lexicon generated: %d actions, %d entities, %d fields",
                  lexicon.getActions() != null ? lexicon.getActions().size() : 0,
                  lexicon.getEntities() != null ? lexicon.getEntities().size() : 0,
                  lexicon.getFields() != null ? lexicon.getFields().size() : 0));
        } catch (Exception ex) {
          log.error("Failed to auto-generate lexicon", ex);
          throw new ExecutionException("Lexicon not found and failed to auto-generate: " + 
              ex.getMessage() + ". You can manually run 'onemcp lexify' to generate it.", ex);
        }
      }
      
      // ─────────────────────────────────────────────────────────────────────
      // STEP 1: NORMALIZE - Convert prompt to PromptSchema
      // ─────────────────────────────────────────────────────────────────────
      // Note: Phase boundaries are logged by PromptSchemaNormalizer with attempt numbers
      StdoutUtility.printNewLine(oneMcp, "Normalizing prompt...");
      long normalizeStart = System.currentTimeMillis();
      
      PromptSchemaNormalizer normalizer = new PromptSchemaNormalizer(oneMcp);
      PromptSchema schema = normalizer.normalize(prompt.trim(), lexicon);
      
      // Check for failure (operation is null)
      // Check action (spec v21) or fall back to operation (legacy)
      String action = schema != null ? schema.getAction() : null;
      if (schema == null || action == null || action.trim().isEmpty()) {
        String errorMessage = schema != null && schema.getNote() != null 
            ? schema.getNote() 
            : "The prompt does not appear to be related to the operations supported by the API";
        long normalizeDuration = System.currentTimeMillis() - normalizeStart;
        ExecutionException ex = new ExecutionException(errorMessage);
        StdoutUtility.printError(oneMcp, errorMessage, ex);
        log.warn("Normalization failed for prompt: {} - {}", prompt, errorMessage);
        throw ex;
      }
      
      if (schema.getCacheKey() == null) {
        throw new ExecutionException("Cache key is null after normalization");
      }
      
      long normalizeDuration = System.currentTimeMillis() - normalizeStart;
      StdoutUtility.printSuccessLine(oneMcp,
          "Normalization completed in (%d ms). PSK: %s".formatted(normalizeDuration, schema.getCacheKey()));
      log.info("Normalized prompt to PSK: {} in {}ms", schema.getCacheKey(), normalizeDuration);
      
      // Log normalized schema for reporting
      try {
        String schemaJson = JacksonUtility.getJsonMapper().writeValueAsString(schema);
        oneMcp.inferenceLogger().logNormalizedPromptSchemaForExecution(schemaJson, normalizeDuration, executionId);
      } catch (Exception e) {
        log.debug("Failed to log normalized schema", e);
      }
      
      // ─────────────────────────────────────────────────────────────────────
      // STEP 2: CACHE LOOKUP
      // ─────────────────────────────────────────────────────────────────────
      ExecutionPlanCache cache = new ExecutionPlanCache(handbookPath);
      long cacheLookupStart = System.currentTimeMillis();
      String cacheKey = schema.getCacheKey();
      java.util.Optional<ExecutionPlanCache.CachedPlanResult> cachedResult = cache.lookupByCacheKey(cacheKey);
      long cacheLookupDuration = System.currentTimeMillis() - cacheLookupStart;
      
      ExecutionPlan plan = null;
      boolean cacheHit = cachedResult.isPresent();
      boolean shouldRegenerate = false;
      boolean cachedPlanFailed = false;
      String cachedPlanError = null;
      int cachedFailedAttempts = 0;
      int maxRetryAttempts = 3; // Maximum number of times to retry failed plans
      
      if (cacheHit) {
        ExecutionPlanCache.CachedPlanResult cached = cachedResult.get();
        if (!cached.executionSucceeded) {
          cachedPlanFailed = true;
          cachedPlanError = cached.lastError;
          cachedFailedAttempts = cached.failedAttempts;
          // Cached plan previously failed - check if we should retry
          if (cached.failedAttempts < maxRetryAttempts) {
            log.info("Cached plan for PSK: {} previously failed ({} attempts), regenerating...", 
                schema.getCacheKey(), cached.failedAttempts);
            StdoutUtility.printNewLine(oneMcp, 
                "Cached plan failed previously (attempt {}), regenerating...".formatted(cached.failedAttempts));
            shouldRegenerate = true;
          } else {
            log.warn("Cached plan for PSK: {} has failed {} times (max: {}), not retrying", 
                schema.getCacheKey(), cached.failedAttempts, maxRetryAttempts);
            StdoutUtility.printError(oneMcp, 
                "Plan has failed {} times, not retrying".formatted(cached.failedAttempts), null);
            throw new ExecutionException(
                "Cached plan has failed " + cached.failedAttempts + " times. Last error: " + 
                (cached.lastError != null ? cached.lastError : "Unknown error"));
          }
        } else {
          // Cached plan succeeded previously - use it
          plan = cached.plan;
          StdoutUtility.printSuccessLine(oneMcp, 
              "✓ CACHE HIT - Reusing cached plan for: %s".formatted(schema.getCacheKey()));
          log.info("Cache HIT for PSK: {}", schema.getCacheKey());
          
          // Don't log plan here - it will be logged in the execute phase
        }
      }
      
      // Log cache lookup result to report (cache status is logged in execution plan event)
      
      // If we're regenerating, treat as cache miss for the rest of the flow
      if (shouldRegenerate) {
        cacheHit = false;
        plan = null;
      }
      
      // Track if this is a newly generated plan (not from cache)
      boolean isNewlyGenerated = (plan == null);
      String previousExecutionError = null;
      int planGenerationAttempts = 0;
      int maxPlanGenerationAttempts = 3;
      boolean executionAttempted = false;
      
      // Always execute at least once if we have a plan, then retry if needed
      // Loop continues if: (1) need to generate plan, OR (2) execution failed and we can retry
      while (plan == null || !executionAttempted || (planGenerationAttempts < maxPlanGenerationAttempts && previousExecutionError != null && result == null)) {
        if (plan == null) {
          // ─────────────────────────────────────────────────────────────────
          // CACHE MISS OR FAILED PLAN - Generate new plan (LLM CALL WILL HAPPEN)
          // ─────────────────────────────────────────────────────────────────
          int planAttempt = planGenerationAttempts + 1;
          oneMcp.inferenceLogger().logPhaseBegin("plan-dag", planAttempt);
          StdoutUtility.printNewLine(oneMcp, "Generating execution plan...");
          if (previousExecutionError != null) {
            StdoutUtility.printNewLine(oneMcp, 
                "Retrying plan generation (attempt {}/{}) with execution error feedback...".formatted(
                    planGenerationAttempts + 1, maxPlanGenerationAttempts));
          }
          long planStart = System.currentTimeMillis();
          
          // STEP 3: CREATE PLAN (this will trigger LLM call)
          try {
            // Determine cache directory using same logic as ExecutionPlanCache
            Path cacheDir = determineCacheDirectory(handbookPath);
            Path endpointInfoDir = cacheDir.resolve("endpoint_info");
            DagPlanner dagPlanner = new DagPlanner(oneMcp, endpointInfoDir);
            String dagPlanJson = dagPlanner.planDag(schema, prompt.trim(), previousExecutionError);
            
            // Convert DAG JSON to ExecutionPlan
            plan = ExecutionPlan.fromJson(dagPlanJson);
            planGenerationAttempts++;
                
            long planDuration = System.currentTimeMillis() - planStart;
            StdoutUtility.printSuccessLine(oneMcp,
                "Plan generation completed in (%d ms).".formatted(planDuration));
            log.info("Generated plan for PSK: {} in {}ms (attempt {})", 
                schema.getCacheKey(), planDuration, planGenerationAttempts);
            
            // Plan generation is logged in execution plan event
            oneMcp.inferenceLogger().logPhaseEnd("plan-dag", planAttempt);
          } catch (Exception e) {
            // Capture plan generation errors
            planGenerationAttempts++;
            String planError = getFullErrorMessage(e);
            log.error("Plan generation failed for PSK: {} (attempt {})", 
                schema.getCacheKey(), planGenerationAttempts, e);
            
            // Automatically log exception with current phase and attempt
            oneMcp.inferenceLogger().logException(e);
            
            // If we haven't exceeded retry limit, retry with error feedback
            if (planGenerationAttempts < maxPlanGenerationAttempts) {
              oneMcp.inferenceLogger().logPhaseEnd("plan-dag", planAttempt);
              previousExecutionError = planError;
              plan = null; // Reset to trigger regeneration
              log.info("Retrying plan generation with error feedback: {}", planError);
              continue; // Retry plan generation
            } else {
              // Exceeded retry limit - throw with proper error message
              oneMcp.inferenceLogger().logPhaseEnd("plan-dag", planAttempt);
              throw new ExecutionException("Plan generation failed after " + planGenerationAttempts + 
                  " attempts: " + planError, e);
            }
          }
        }
        
        // ─────────────────────────────────────────────────────────────────────
        // STEP 4: EXECUTE PLAN (with values from PromptSchema)
        // ─────────────────────────────────────────────────────────────────────
        executionAttempted = true;
        oneMcp.inferenceLogger().logPhaseBegin("execute", planGenerationAttempts);
        StdoutUtility.printNewLine(oneMcp, "Executing plan...");
        
        // Log the execution plan to the report with cache hit status
        // Note: cacheHit reflects whether we got the plan from cache (even if we're regenerating due to failure)
        // Ensure plan is logged even if serialization fails
        // Also log the Prompt Schema for debugging
        String schemaJson = null;
        try {
          schemaJson = JacksonUtility.getJsonMapper().writeValueAsString(schema);
        } catch (Exception e) {
          log.debug("Failed to serialize schema for logging (PSK: {}): {}", schema.getCacheKey(), e.getMessage());
        }
        
        if (plan != null) {
          try {
            String planJson = plan.toPrettyJson();
            oneMcp.inferenceLogger().logExecutionPlan(planJson, cacheHit && !shouldRegenerate, schemaJson);
          } catch (Exception e) {
            log.error("Failed to serialize plan for logging (PSK: {}): {}", schema.getCacheKey(), e.getMessage(), e);
            // Log with error message so we know the plan exists but couldn't be serialized
            oneMcp.inferenceLogger().logExecutionPlan(
                "{\"error\": \"Failed to serialize plan: " + e.getMessage() + "\"}", 
                cacheHit && !shouldRegenerate, schemaJson);
          }
        } else {
          log.error("Cannot log execution plan: plan is null (PSK: {})", schema.getCacheKey());
          // Log empty plan so we know execution was attempted but plan was null
          oneMcp.inferenceLogger().logExecutionPlan("", cacheHit && !shouldRegenerate, schemaJson);
        }
        
        long executeStart = System.currentTimeMillis();
        
        boolean executionSucceeded = false;
        String executionError = null;
        
        try {
          OperationRegistry registry = buildOperationRegistry(ctx);
          ExecutionPlanEngine engine = new ExecutionPlanEngine(JacksonUtility.getJsonMapper(), registry);
          
          // Build initial state from PromptSchema params
          ObjectNode initialState = JacksonUtility.getJsonMapper().createObjectNode();
          if (schema.getParams() != null) {
            for (Map.Entry<String, Object> entry : schema.getParams().entrySet()) {
              initialState.set(entry.getKey(), JacksonUtility.getJsonMapper().valueToTree(entry.getValue()));
            }
          }
          
          JsonNode planJson = JacksonUtility.getJsonMapper().readTree(plan.toJson());
          JsonNode executionResult = engine.execute(planJson, initialState);
          result = executionResult != null ? executionResult.toString() : "{}";
          executionSucceeded = true;
          oneMcp.inferenceLogger().logPhaseEnd("execute");
          log.info("Execution succeeded for PSK: {}, result length: {}", 
              schema.getCacheKey(), result != null ? result.length() : 0);
        } catch (Exception e) {
          // Preserve full error message including cause
          executionError = getFullErrorMessage(e);
          
          // Log the exception (automatically includes phase, attempt, and stack trace)
          log.error("Plan execution failed for PSK: {} (attempt {}): {}", 
              schema.getCacheKey(), planGenerationAttempts, executionError, e);
          oneMcp.inferenceLogger().logException(e);
          
          oneMcp.inferenceLogger().logPhaseEnd("execute");
          
          // Store the failed plan BEFORE resetting it (if we're not retrying)
          // This ensures we cache failed plans for retry tracking
          if (!isNewlyGenerated || planGenerationAttempts >= maxPlanGenerationAttempts) {
            // Not retrying - store the failed plan
            if (plan != null) {
              try {
                cache.storeByCacheKey(schema.getCacheKey(), plan, false, executionError);
                log.debug("Stored failed plan in cache for PSK: {} (attempt {})", 
                    schema.getCacheKey(), planGenerationAttempts);
              } catch (Exception cacheEx) {
                log.error("Failed to store failed plan in cache for PSK: {}", schema.getCacheKey(), cacheEx);
              }
            }
          }
          
          // Always set previousExecutionError so it's available for error reporting
          previousExecutionError = executionError;
          
          // If this is a newly generated plan and we haven't exceeded retry limit, retry
          if (isNewlyGenerated && planGenerationAttempts < maxPlanGenerationAttempts) {
            plan = null; // Reset plan to trigger regeneration
            log.info("Retrying plan generation with execution error feedback: {}", executionError);
            continue; // Retry plan generation
          }
          
          // If this is a cached plan that failed, we should also retry by regenerating
          if (!isNewlyGenerated && planGenerationAttempts < maxPlanGenerationAttempts) {
            plan = null; // Reset plan to trigger regeneration
            log.info("Cached plan failed, regenerating with execution error feedback: {}", executionError);
            continue; // Retry plan generation
          }
        } finally {
          long executeDuration = System.currentTimeMillis() - executeStart;
          
          // ─────────────────────────────────────────────────────────────────────
          // STEP 5: STORE PLAN (always store, with execution status)
          // ─────────────────────────────────────────────────────────────────────
          // Only store if execution succeeded (failed plans are stored in catch block)
          if (executionSucceeded) {
            if (plan == null) {
              log.warn("Cannot store plan: plan is null (PSK: {})", schema.getCacheKey());
            } else {
              try {
                log.info("Storing plan in cache for PSK: {} (execution succeeded)", schema.getCacheKey());
                cache.storeByCacheKey(schema.getCacheKey(), plan, true, null);
                log.info("Cache storage completed for PSK: {} (succeeded: true)", 
                    schema.getCacheKey());
      } catch (Exception e) {
                log.error("Failed to store plan in cache for PSK: {}", schema.getCacheKey(), e);
              }
            }
          } else {
            log.debug("Skipping cache storage - execution did not succeed (PSK: {}, attempts: {})", 
                schema.getCacheKey(), planGenerationAttempts);
          }
          
          if (executionSucceeded) {
            StdoutUtility.printSuccessLine(oneMcp,
                "Execution completed in (%d ms).".formatted(executeDuration));
            StdoutUtility.printSuccessLine(oneMcp, 
                "✓ Plan cached for future use: %s".formatted(schema.getCacheKey()));
            log.info("Cached plan for PSK: {} after successful execution", schema.getCacheKey());
          } else if (planGenerationAttempts >= maxPlanGenerationAttempts) {
            StdoutUtility.printError(oneMcp, 
                "Execution failed in (%d ms)".formatted(executeDuration), null);
            log.info("Cached failed plan for PSK: {} (will retry on next attempt)", schema.getCacheKey());
          }
          
          // Plan execution is logged in execution plan event
        }
        
        // If execution succeeded, break out of retry loop
        if (executionSucceeded) {
          break;
        }
        
        // If we've exhausted retries, break and throw error
        if (planGenerationAttempts >= maxPlanGenerationAttempts) {
          break;
        }
      }
      
      // Re-throw exception if execution failed after all retries
      if (result == null) {
        String finalError = previousExecutionError != null ? previousExecutionError : "Unknown error";
        
        log.error("Final error after all retries (PSK: {}, attempts: {}): {}", 
            schema.getCacheKey(), planGenerationAttempts, finalError);
        
        // Preserve the original error message without extra wrapping if it's already descriptive
        if (finalError.startsWith("Operation") && finalError.contains("failed")) {
          throw new ExecutionException(finalError);
        } else {
          throw new ExecutionException("Plan execution failed after " + planGenerationAttempts + " attempts: " + finalError);
        }
      }
      
      // Final summary
      long totalDuration = System.currentTimeMillis() - start;
      String hitOrMiss = cacheHit ? "CACHE HIT" : "CACHE MISS";
      StdoutUtility.printSuccessLine(oneMcp,
          "Assignment handled in (%d ms) [%s]\nAnswer:\n%s"
              .formatted(totalDuration, hitOrMiss, StringUtility.formatWithIndent(result, 4)));
      
      success = true;
      
    } catch (Exception e) {
      log.error("Failed to handle prompt: {}", e.getMessage(), e);
      // Include full error details including cause
      String fullError = getFullErrorMessage(e);
      // Don't add "Error: " prefix if the message already starts with it or is descriptive
      if (fullError.startsWith("Error:") || fullError.startsWith("Plan execution failed:") || 
          fullError.startsWith("Operation") || fullError.startsWith("Filter with")) {
        result = fullError;
      } else {
        result = "Error: " + fullError;
      }
      StdoutUtility.printError(oneMcp, "Failed to handle prompt", e);
    }

    // Log final response
    if (result != null) {
      oneMcp.inferenceLogger().logFinalResponse(result);
    }
    
    // Complete execution tracking
    long durationMs = System.currentTimeMillis() - start;
    String finalReportPath = oneMcp.inferenceLogger().completeExecution(executionId, durationMs, success, start);
    
    // Return JSON with content and reportPath if report is available
    if (finalReportPath != null && !finalReportPath.isEmpty()) {
      try {
        com.fasterxml.jackson.databind.ObjectMapper mapper = JacksonUtility.getJsonMapper();
        java.util.Map<String, String> response = new java.util.HashMap<>();
        response.put("content", result != null ? result : "");
        response.put("reportPath", finalReportPath);
        return mapper.writeValueAsString(response);
      } catch (Exception e) {
        log.warn("Failed to serialize response with report path", e);
      }
    }
    
    return result;
  }

  /**
   * Determine cache directory using same priority as ExecutionPlanCache:
   * 1. ONEMCP_CACHE_DIR environment variable
   * 2. ONEMCP_HOME_DIR/cache
   * 3. User home/.onemcp/cache
   * 4. Fallback to handbook/cache
   */
  private Path determineCacheDirectory(Path handbookPath) {
    // Priority 1: ONEMCP_CACHE_DIR environment variable
    String cacheDirEnv = System.getenv("ONEMCP_CACHE_DIR");
    if (cacheDirEnv != null && !cacheDirEnv.isBlank()) {
      Path cacheDir = Path.of(cacheDirEnv);
      log.debug("Using cache directory from ONEMCP_CACHE_DIR: {}", cacheDir);
      return cacheDir;
    }

    // Priority 2: ONEMCP_HOME_DIR/cache
    String homeDir = System.getenv("ONEMCP_HOME_DIR");
    if (homeDir != null && !homeDir.isBlank()) {
      Path cacheDir = Path.of(homeDir, "cache");
      log.debug("Using cache directory from ONEMCP_HOME_DIR: {}", cacheDir);
      return cacheDir;
    }

    // Priority 3: User home/.onemcp/cache (fallback)
    String userHome = System.getProperty("user.home");
    if (userHome != null && !userHome.isBlank()) {
      Path cacheDir = Path.of(userHome, ".onemcp", "cache");
      log.debug("Using cache directory from user home: {}", cacheDir);
      return cacheDir;
    }

    // Final fallback: handbook/cache (should not happen in normal operation)
    if (handbookPath != null) {
      log.warn("Could not determine cache directory, falling back to handbook/cache");
      return handbookPath.resolve("cache");
    }
    
    // Ultimate fallback: temp directory
    Path cacheDir = Path.of(System.getProperty("java.io.tmpdir"), "onemcp", "cache");
    log.warn("Could not determine cache directory, using fallback: {}", cacheDir);
    return cacheDir;
  }

  /**
   * Get the handbook path from environment or knowledge base.
   */
  private Path getHandbookPath(OrchestratorContext ctx) {
    // First try HANDBOOK_DIR environment variable (set by CLI)
    String envHandbookDir = System.getenv("HANDBOOK_DIR");
    if (envHandbookDir != null && !envHandbookDir.isBlank()) {
      Path envHandbookPath = Path.of(envHandbookDir);
      if (Files.exists(envHandbookPath) && Files.isDirectory(envHandbookPath)) {
        log.debug("Using HANDBOOK_DIR for cache flow: {}", envHandbookPath);
        return envHandbookPath;
      }
    }
    
    // Fallback to Handbook path
    Handbook handbook = ctx.handbook();
    if (handbook != null && handbook.location() != null) {
      log.debug("Using Handbook path for cache flow: {}", handbook.location());
      return handbook.location();
    }
    
    log.debug("No handbook path available for cache flow");
    return null;
  }

  /**
   * Build an OperationRegistry from Handbook services.
   * Each service's operations are registered as invokable functions.
   */
  private OperationRegistry buildOperationRegistry(OrchestratorContext ctx) {
    OperationRegistry registry = new OperationRegistry();
    Handbook handbook = ctx.handbook();
    
    if (handbook == null) {
      log.warn("No handbook available for operation registry");
      return registry;
    }
    
    Path handbookPath = handbook.location();
    if (handbookPath == null) {
      log.warn("Handbook location is not available");
      return registry;
    }
    
    // Get base URL from configuration (default to localhost:8080) - will be overridden by OpenAPI spec if available
    String baseUrl = oneMcp.configuration().getString("server.baseUrl", "http://localhost:8080");
    AtomicInteger operationCount = new AtomicInteger(0);
    
    try {
      // Load OpenAPI spec from handbook
      Path apisDir = handbookPath.resolve("apis");
      Path openapiDir = handbookPath.resolve("openapi");
      Path sourceDir = null;
      
      if (Files.exists(apisDir)) {
        sourceDir = apisDir;
      } else if (Files.exists(openapiDir)) {
        sourceDir = openapiDir;
      } else {
        log.warn("No APIs directory found in handbook: {}", handbookPath);
        return registry;
      }
      
      // Find first OpenAPI spec file
      Path specFile = null;
      try (var stream = Files.list(sourceDir)) {
        specFile = stream
            .filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml") || p.toString().endsWith(".json"))
            .findFirst()
            .orElse(null);
      }
      
      if (specFile == null) {
        log.warn("No OpenAPI spec file found in: {}", sourceDir);
        return registry;
      }
      
      // Load OpenAPI spec
      io.swagger.v3.oas.models.OpenAPI openAPI = com.gentoro.onemcp.openapi.OpenApiLoader.load(specFile.toString());
      
      // Prefer baseUrl from OpenAPI spec servers (contains correct path prefix like /acme)
      // Fall back to configuration baseUrl if no server URL in spec
      if (openAPI.getServers() != null && !openAPI.getServers().isEmpty()) {
        io.swagger.v3.oas.models.servers.Server server = openAPI.getServers().get(0);
        if (server != null && server.getUrl() != null && !server.getUrl().isEmpty()) {
          baseUrl = server.getUrl();
          log.debug("Using baseUrl from OpenAPI spec: {}", baseUrl);
        }
      }
      
      // Build OpenAPI proxy to handle operation invocations
      // Pass inference logger so API calls can be logged
      OpenApiProxy proxy = new OpenApiProxyImpl(openAPI, baseUrl, oneMcp.inferenceLogger());
      
      // Register each operation from the OpenAPI spec
      if (openAPI.getPaths() != null) {
        openAPI.getPaths().forEach((path, pathItem) -> {
          pathItem.readOperationsMap().forEach((httpMethod, operation) -> {
            String operationId = operation.getOperationId();
            if (operationId != null && !operationId.isEmpty()) {
              registry.register(operationId, (input) -> {
                try {
                  return proxy.invoke(operationId, input);
                } catch (Exception e) {
                  log.error("Failed to invoke operation: {}", operationId, e);
                  throw new ExecutionPlanException("Operation '" + operationId + "' failed: " + e.getMessage(), e);
                }
              });
              operationCount.incrementAndGet();
              log.debug("Registered operation: {}", operationId);
            }
          });
        });
      }
    } catch (Exception e) {
      log.warn("Failed to build operation registry", e);
    }
    
    log.info("Built operation registry with {} operations", operationCount.get());
    return registry;
  }

  /**
   * Extract full error message including cause chain.
   * This helps preserve underlying error details that might be lost in exception wrapping.
   * Recursively walks the cause chain to extract all error messages.
   */
  private String getFullErrorMessage(Exception e) {
    if (e == null) {
      return "Unknown error";
    }
    
    StringBuilder sb = new StringBuilder();
    Throwable current = e;
    int depth = 0;
    int maxDepth = 10; // Prevent infinite loops
    
    while (current != null && depth < maxDepth) {
      String message = current.getMessage();
      if (message == null || message.isEmpty()) {
        message = current.getClass().getSimpleName();
      }
      
      if (depth == 0) {
        // First level: exception type and message
        sb.append(current.getClass().getSimpleName());
        if (!message.equals(current.getClass().getSimpleName())) {
          sb.append(": ").append(message);
        }
      } else {
        // Nested causes
        sb.append(" -> ").append(current.getClass().getSimpleName());
        if (!message.equals(current.getClass().getSimpleName())) {
          sb.append(": ").append(message);
        }
      }
      
      current = current.getCause();
      depth++;
      
      // Stop if we've reached the root cause or if the cause is the same as the current exception
      if (current != null && current == e) {
        break;
      }
    }
    
    String result = sb.toString();
    if (result.isEmpty()) {
      return e.getClass().getSimpleName();
    }
    
    return result;
  }


  /**
   * Index schema from handbook (used by MCP server).
   * Clears existing lexicon and execution plans, then executes the lexifier to regenerate the conceptual lexicon.
   * @return JSON string with status messages, report path, and final result
   */
  public String indexSchema() {
    try {
      log.info("Starting index operation: clearing cache and regenerating lexicon...");
      
      // Start execution tracking
      String executionId = java.util.UUID.randomUUID().toString();
      String reportPath = oneMcp.inferenceLogger().startExecution(executionId, "index schema (lexifier)");
      long startTime = System.currentTimeMillis();
      
      java.util.List<String> statusMessages = new java.util.ArrayList<>();
      
      // Step 1: Clear existing lexicon and execution plans
      statusMessages.add("Clearing existing lexicon and execution plans...");
      Path handbookPath = getHandbookPath(new OrchestratorContext(oneMcp, new ValueStore()));
      if (handbookPath == null) {
        throw new ExecutionException("No handbook path available. Set HANDBOOK_DIR or configure a handbook.");
      }
      
      // Clear execution plan cache
      ExecutionPlanCache planCache = new ExecutionPlanCache(handbookPath);
      int plansCleared = planCache.clear();
      log.info("Cleared {} execution plan(s)", plansCleared);
      statusMessages.add(String.format("✓ Cleared %d execution plan(s)", plansCleared));
      
      // Clear lexicon file
      Lexifier lexifier = new Lexifier(oneMcp);
      java.nio.file.Path cacheDir = lexifier.getCacheDirectory();
      java.nio.file.Path lexiconPath = cacheDir.resolve("lexicon.json");
      boolean lexiconDeleted = false;
      if (java.nio.file.Files.exists(lexiconPath)) {
        try {
          java.nio.file.Files.delete(lexiconPath);
          lexiconDeleted = true;
          log.info("Deleted existing lexicon: {}", lexiconPath);
          statusMessages.add("✓ Deleted existing lexicon");
        } catch (java.io.IOException e) {
          log.warn("Failed to delete lexicon file: {}", lexiconPath, e);
          statusMessages.add("⚠ Failed to delete lexicon file (continuing anyway)");
        }
      } else {
        statusMessages.add("ℹ No existing lexicon found");
      }
      
      // Step 2: Execute lexifier to regenerate the conceptual lexicon
      statusMessages.add("Executing lexifier to regenerate lexicon...");
      log.info("Executing lexifier to regenerate lexicon...");
      
      ConceptualLexicon lexicon = lexifier.extractLexicon();
      
      long duration = System.currentTimeMillis() - startTime;
      int actionCount = lexicon.getActions() != null ? lexicon.getActions().size() : 0;
      int entityCount = lexicon.getEntities() != null ? lexicon.getEntities().size() : 0;
      int fieldCount = lexicon.getFields() != null ? lexicon.getFields().size() : 0;
      
      log.info("Lexicon regenerated successfully in {}ms: {} actions, {} entities, {} fields", 
          duration, actionCount, entityCount, fieldCount);
      
      String successMessage = String.format("✓ Lexicon regenerated: %d actions, %d entities, %d fields (took %dms)", 
          actionCount, entityCount, fieldCount, duration);
      statusMessages.add(successMessage);
      
      // Complete lexifier report first (this will set currentReportPath to lexifier report)
      String lexifierReportPath = null;
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().endLexifierReport(true, null);
        // Get the lexifier report path (it was set by endLexifierReport)
        lexifierReportPath = oneMcp.inferenceLogger().getCurrentReportPath();
      }
      
      // Complete execution report (this will overwrite currentReportPath with execution report)
      oneMcp.inferenceLogger().completeExecution(executionId, duration, true, startTime);
      
      // Use lexifier report path if available (it has all the detailed information)
      // Otherwise fall back to execution report path
      String finalReportPath = lexifierReportPath != null ? lexifierReportPath : reportPath;
      
      // Build response JSON with status messages
      java.util.Map<String, Object> response = new java.util.HashMap<>();
      response.put("status", "success");
      response.put("messages", statusMessages);
      response.put("content", successMessage);
      response.put("reportPath", finalReportPath);
      response.put("stats", java.util.Map.of(
          "actions", actionCount,
          "entities", entityCount,
          "fields", fieldCount,
          "durationMs", duration
      ));
      
      return JacksonUtility.toJson(response);
    } catch (Exception e) {
      log.error("Failed to execute lexifier", e);
      // Complete lexifier report with error
      if (oneMcp.inferenceLogger() != null) {
        oneMcp.inferenceLogger().endLexifierReport(false, e.getMessage());
      }
      
      // Build error response JSON
      java.util.Map<String, Object> errorResponse = new java.util.HashMap<>();
      errorResponse.put("status", "error");
      errorResponse.put("content", "Failed to execute lexifier: " + e.getMessage());
      errorResponse.put("message", "Failed to execute lexifier: " + e.getMessage());
      
      String reportPath = oneMcp.inferenceLogger().getCurrentReportPath();
      if (reportPath != null) {
        errorResponse.put("reportPath", reportPath);
      }
      
      throw new ExecutionException(JacksonUtility.toJson(errorResponse), e);
    }
  }
}
