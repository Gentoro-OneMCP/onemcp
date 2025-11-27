package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.cache.DictionaryExtractorService;
import com.gentoro.onemcp.cache.ExecutionPlan;
import com.gentoro.onemcp.cache.ExecutionPlanCache;
import com.gentoro.onemcp.cache.PromptDictionary;
import com.gentoro.onemcp.cache.PromptSchema;
import com.gentoro.onemcp.cache.PromptSchemaNormalizer;
import com.gentoro.onemcp.cache.PromptSchemaStep;
import com.gentoro.onemcp.cache.PromptSchemaWorkflow;
import com.gentoro.onemcp.cache.SchemaAwarePlanGenerator;
import com.gentoro.onemcp.context.KnowledgeBase;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.memory.ValueStore;
import com.gentoro.onemcp.openapi.OpenApiProxy;
import com.gentoro.onemcp.openapi.OpenApiProxyImpl;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StdoutUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.nio.file.Files;
import java.nio.file.Path;
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
      
      // Load dictionary
      DictionaryExtractorService extractor = new DictionaryExtractorService(oneMcp);
      Path dictionaryPath = handbookPath.resolve("apis").resolve("dictionary.yaml");
      PromptDictionary dictionary = extractor.loadDictionary(dictionaryPath);
      
      if (dictionary == null) {
        throw new ExecutionException("Dictionary not found at: " + dictionaryPath + 
            ". Run 'onemcp create-dictionary' first.");
      }
      
      // ─────────────────────────────────────────────────────────────────────
      // STEP 1: NORMALIZE - Convert prompt to PromptSchema
      // ─────────────────────────────────────────────────────────────────────
      oneMcp.inferenceLogger().logPhaseChange("normalize");
      StdoutUtility.printNewLine(oneMcp, "Normalizing prompt...");
      long normalizeStart = System.currentTimeMillis();
      
      PromptSchemaNormalizer normalizer = new PromptSchemaNormalizer(oneMcp);
      PromptSchemaWorkflow workflow = normalizer.normalize(prompt.trim(), dictionary);
      
      if (workflow == null || workflow.getSteps() == null || workflow.getSteps().isEmpty()) {
        throw new ExecutionException("Normalization produced empty workflow");
      }
      
      // For now, we only support single-step workflows
      PromptSchemaStep step = workflow.getSteps().get(0);
      PromptSchema schema = step.getPs();
      
      if (schema == null || schema.getCacheKey() == null) {
        throw new ExecutionException("Schema or cache key is null after normalization");
        }
      
      long normalizeDuration = System.currentTimeMillis() - normalizeStart;
      StdoutUtility.printSuccessLine(oneMcp,
          "Normalization completed in (%d ms). PSK: %s".formatted(normalizeDuration, schema.getCacheKey()));
      log.info("Normalized prompt to PSK: {} in {}ms", schema.getCacheKey(), normalizeDuration);
      
      // Log normalized schema for reporting
      try {
        String schemaJson = JacksonUtility.getJsonMapper().writeValueAsString(workflow);
        oneMcp.inferenceLogger().logNormalizedPromptSchemaForExecution(schemaJson, normalizeDuration, executionId);
          } catch (Exception e) {
        log.debug("Failed to log normalized schema", e);
          }
      
      // ─────────────────────────────────────────────────────────────────────
      // STEP 2: CACHE LOOKUP
      // ─────────────────────────────────────────────────────────────────────
      ExecutionPlanCache cache = new ExecutionPlanCache(handbookPath);
      long cacheLookupStart = System.currentTimeMillis();
      java.util.Optional<ExecutionPlanCache.CachedPlanResult> cachedResult = cache.lookupWithMetadata(schema);
      long cacheLookupDuration = System.currentTimeMillis() - cacheLookupStart;
      
      ExecutionPlan plan = null;
      boolean cacheHit = cachedResult.isPresent();
      boolean shouldRegenerate = false;
      int maxRetryAttempts = 3; // Maximum number of times to retry failed plans
      
      // Log cache lookup result to report
      oneMcp.inferenceLogger().logCacheLookup(cacheHit, schema.getCacheKey(), cacheLookupDuration);
      
      if (cacheHit) {
        ExecutionPlanCache.CachedPlanResult cached = cachedResult.get();
        if (!cached.executionSucceeded) {
          // Cached plan previously failed - check if we should retry
          if (cached.failedAttempts < maxRetryAttempts) {
            log.info("Cached plan for PSK: {} previously failed ({} attempts), regenerating...", 
                schema.getCacheKey(), cached.failedAttempts);
            StdoutUtility.printNewLine(oneMcp, 
                "Cached plan failed previously (attempt {}), regenerating...".formatted(cached.failedAttempts));
            shouldRegenerate = true;
            cacheHit = false; // Treat as cache miss for regeneration
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
          oneMcp.inferenceLogger().logPhaseChange("cache_hit");
          StdoutUtility.printSuccessLine(oneMcp, 
              "✓ CACHE HIT - Reusing cached plan for: %s".formatted(schema.getCacheKey()));
          log.info("Cache HIT for PSK: {}", schema.getCacheKey());
        }
      }
      
      if (plan == null) {
        // ─────────────────────────────────────────────────────────────────
        // CACHE MISS OR FAILED PLAN - Generate new plan (LLM CALL WILL HAPPEN)
        // ─────────────────────────────────────────────────────────────────
        oneMcp.inferenceLogger().logPhaseChange("plan");
        StdoutUtility.printNewLine(oneMcp, "Generating execution plan...");
        long planStart = System.currentTimeMillis();
        
        // STEP 3: CREATE PLAN (this will trigger LLM call)
        SchemaAwarePlanGenerator generator = new SchemaAwarePlanGenerator(ctx);
        plan = generator.createPlan(prompt.trim(), schema);
            
        long planDuration = System.currentTimeMillis() - planStart;
        StdoutUtility.printSuccessLine(oneMcp,
            "Plan generation completed in (%d ms).".formatted(planDuration));
        log.info("Generated plan for PSK: {} in {}ms", schema.getCacheKey(), planDuration);
        
        // Log plan generation event
        oneMcp.inferenceLogger().logPlanGeneration(schema.getCacheKey(), planDuration);
      }
      
      // ─────────────────────────────────────────────────────────────────────
      // STEP 4: EXECUTE PLAN (with values from PromptSchema)
      // ─────────────────────────────────────────────────────────────────────
      oneMcp.inferenceLogger().logPhaseChange("execute");
      StdoutUtility.printNewLine(oneMcp, "Executing plan...");
      long executeStart = System.currentTimeMillis();
      
      boolean executionSucceeded = false;
      String executionError = null;
      
      try {
        OperationRegistry registry = buildOperationRegistry(ctx);
        result = plan.execute(schema, registry);
        executionSucceeded = true;
            } catch (Exception e) {
        executionError = e.getMessage();
        log.error("Plan execution failed for PSK: {}", schema.getCacheKey(), e);
        // Don't re-throw yet - we need to store the failed plan first
      } finally {
        long executeDuration = System.currentTimeMillis() - executeStart;
        
        // ─────────────────────────────────────────────────────────────────────
        // STEP 5: STORE PLAN (always store, with execution status)
        // ─────────────────────────────────────────────────────────────────────
        cache.store(schema, plan, executionSucceeded, executionError);
        
        if (executionSucceeded) {
          StdoutUtility.printSuccessLine(oneMcp,
              "Execution completed in (%d ms).".formatted(executeDuration));
          StdoutUtility.printSuccessLine(oneMcp, 
              "✓ Plan cached for future use: %s".formatted(schema.getCacheKey()));
          log.info("Cached plan for PSK: {} after successful execution", schema.getCacheKey());
        } else {
          StdoutUtility.printError(oneMcp, 
              "Execution failed in (%d ms)".formatted(executeDuration), null);
          log.info("Cached failed plan for PSK: {} (will retry on next attempt)", schema.getCacheKey());
        }
        
        // Log plan execution event (distinguish cache hit vs miss)
        if (cacheHit && !shouldRegenerate) {
          oneMcp.inferenceLogger().logPlanExecution(schema.getCacheKey(), executeDuration);
        }
      }
      
      // Re-throw exception if execution failed
      if (!executionSucceeded) {
        throw new ExecutionException("Plan execution failed: " + executionError);
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
      result = "Error: " + e.getMessage();
      StdoutUtility.printError(oneMcp, "Failed to handle prompt", e);
    }

    // Log final response
    if (result != null) {
      oneMcp.inferenceLogger().logFinalResponse(result);
    }
    
    // Complete execution tracking
    long durationMs = System.currentTimeMillis() - start;
    String finalReportPath = oneMcp.inferenceLogger().completeExecution(executionId, durationMs, success);
    
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
    
    // Fallback to KnowledgeBase path
    KnowledgeBase knowledgeBase = ctx.knowledgeBase();
    if (knowledgeBase != null && knowledgeBase.handbookPath() != null) {
      log.debug("Using KnowledgeBase path for cache flow: {}", knowledgeBase.handbookPath());
      return knowledgeBase.handbookPath();
    }
    
    log.debug("No handbook path available for cache flow");
    return null;
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
        // Pass inference logger so API calls are logged in the report
        OpenApiProxy proxy = new OpenApiProxyImpl(openAPI, baseUrl, oneMcp.inferenceLogger());
        
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
}
