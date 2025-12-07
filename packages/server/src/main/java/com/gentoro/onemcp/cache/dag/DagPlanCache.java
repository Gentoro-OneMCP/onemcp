package com.gentoro.onemcp.cache.dag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

/**
 * Cache for DAG execution plans.
 * 
 * <p>Stores DAG plans as JSON files in the cache directory.
 * Each cached plan is stored in a file named {@code <cache_key>.dag.json}.
 */
public class DagPlanCache {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(DagPlanCache.class);

  private final Path cacheDir;
  private final ObjectMapper mapper = JacksonUtility.getJsonMapper();

  public DagPlanCache(Path cacheDir) {
    this.cacheDir = cacheDir;
    ensureCacheDirectory();
  }

  private void ensureCacheDirectory() {
    try {
      if (!Files.exists(cacheDir)) {
        Files.createDirectories(cacheDir);
        log.info("Created DAG cache directory: {}", cacheDir);
      }
    } catch (IOException e) {
      log.warn("Failed to create DAG cache directory: {}", cacheDir, e);
    }
  }

  /**
   * Look up a cached DAG plan by cache key.
   * 
   * @param cacheKey the cache key (derived from PS structure)
   * @return Optional containing the cached plan result if found
   */
  public Optional<CachedDagPlanResult> lookupByCacheKey(String cacheKey) {
    if (cacheKey == null || cacheKey.isEmpty()) {
      log.warn("Cannot lookup DAG plan: cacheKey is null or empty");
      return Optional.empty();
    }

    Path planFile = cacheDir.resolve(cacheKey + ".dag.json");

    if (!Files.exists(planFile)) {
      log.debug("DAG Cache MISS for cache key: {}", cacheKey);
      return Optional.empty();
    }

    try {
      String content = Files.readString(planFile);
      CachedDagPlanFile cached = mapper.readValue(content, CachedDagPlanFile.class);

      DagPlan plan = DagPlan.fromJsonString(cached.planJson);

      boolean executionSucceeded = "SUCCESS".equals(cached.executionStatus) ||
          (cached.executionStatus == null && cached.failedAttempts == null);
      String lastError = cached.lastError;
      int failedAttempts = cached.failedAttempts != null ? cached.failedAttempts : 0;

      if (executionSucceeded) {
        log.info("DAG Cache HIT for cache key: {} (cached at {})", cacheKey, cached.createdAt);
      } else {
        log.info("DAG Cache HIT for cache key: {} (cached at {}, FAILED - {} attempts)",
            cacheKey, cached.createdAt, failedAttempts);
      }

      return Optional.of(new CachedDagPlanResult(plan, executionSucceeded, lastError, failedAttempts));
    } catch (Exception e) {
      log.error("Failed to load cached DAG plan for cache key: {}", cacheKey, e);
      // Delete corrupted cache file
      try {
        Files.deleteIfExists(planFile);
        log.info("Deleted corrupted DAG cache file: {}", planFile);
      } catch (IOException ioe) {
        log.warn("Failed to delete corrupted DAG cache file: {}", planFile, ioe);
      }
      return Optional.empty();
    }
  }

  /**
   * Store a DAG plan in the cache.
   * 
   * @param cacheKey the cache key
   * @param plan the DAG plan to cache
   * @param executionSucceeded whether execution succeeded
   * @param errorMessage error message if execution failed (null if succeeded)
   */
  public void storeByCacheKey(String cacheKey, DagPlan plan, boolean executionSucceeded, 
      String errorMessage) {
    if (cacheKey == null || cacheKey.isEmpty()) {
      log.warn("Cannot store DAG plan: cacheKey is null or empty");
      return;
    }
    if (plan == null) {
      log.warn("Cannot store DAG plan: plan is null");
      return;
    }

    Path planFile = cacheDir.resolve(cacheKey + ".dag.json");

    try {
      ensureCacheDirectory();

      // Load existing metadata to preserve failedAttempts count
      int failedAttempts = 0;
      if (Files.exists(planFile)) {
        try {
          String existingContent = Files.readString(planFile);
          CachedDagPlanFile existing = mapper.readValue(existingContent, CachedDagPlanFile.class);
          if (existing.failedAttempts != null) {
            failedAttempts = existing.failedAttempts;
          }
        } catch (Exception e) {
          log.debug("Could not read existing DAG cache metadata, starting fresh", e);
        }
      }

      // Increment failed attempts if execution failed
      if (!executionSucceeded) {
        failedAttempts++;
      } else {
        // Reset on success
        failedAttempts = 0;
      }

      CachedDagPlanFile cached = new CachedDagPlanFile();
      cached.psk = cacheKey;
      cached.createdAt = Instant.now().toString();
      cached.planJson = plan.toJson();
      cached.executionStatus = executionSucceeded ? "SUCCESS" : "FAILED";
      cached.lastError = errorMessage;
      cached.failedAttempts = failedAttempts;

      String content = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cached);
      Files.writeString(planFile, content);

      if (executionSucceeded) {
        log.info("Stored DAG plan in cache: {} ({} nodes)", cacheKey, plan.getNodes().size());
      } else {
        log.info("Stored failed DAG plan in cache: {} (attempt {})", cacheKey, failedAttempts);
      }
    } catch (Exception e) {
      log.error("Failed to store DAG plan in cache: {}", cacheKey, e);
    }
  }

  // Inner classes for cache file structure

  private static class CachedDagPlanFile {
    @JsonProperty("psk")
    public String psk;

    @JsonProperty("created_at")
    public String createdAt;

    @JsonProperty("plan_json")
    public String planJson;

    @JsonProperty("execution_status")
    public String executionStatus;

    @JsonProperty("last_error")
    public String lastError;

    @JsonProperty("failed_attempts")
    public Integer failedAttempts;
  }

  public static class CachedDagPlanResult {
    private final DagPlan plan;
    private final boolean executionSucceeded;
    private final String lastError;
    private final int failedAttempts;

    public CachedDagPlanResult(DagPlan plan, boolean executionSucceeded, String lastError, 
        int failedAttempts) {
      this.plan = plan;
      this.executionSucceeded = executionSucceeded;
      this.lastError = lastError;
      this.failedAttempts = failedAttempts;
    }

    public DagPlan getPlan() {
      return plan;
    }

    public boolean isExecutionSucceeded() {
      return executionSucceeded;
    }

    public String getLastError() {
      return lastError;
    }

    public int getFailedAttempts() {
      return failedAttempts;
    }
  }
}

