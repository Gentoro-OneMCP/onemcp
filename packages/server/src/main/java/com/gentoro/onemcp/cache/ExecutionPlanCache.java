package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.plan.ExecutionPlan;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

/**
 * Cache for execution plans, stored as JSON files in ONEMCP_HOME_DIR/cache.
 *
 * <p>Each cached plan is stored in a file named {@code <cache_key>.json} where
 * the cache key is derived from the {@link PromptSchema}.
 *
 * <p>The cache treats {@link ExecutionPlan} as opaque - it doesn't inspect the
 * plan's contents, just serializes/deserializes it.
 *
 * <p>Cache location priority:
 * <ol>
 *   <li>ONEMCP_CACHE_DIR environment variable (if set)
 *   <li>ONEMCP_HOME_DIR/cache (if ONEMCP_HOME_DIR is set)
 *   <li>User home/.onemcp/cache (fallback)
 * </ol>
 */
public class ExecutionPlanCache {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ExecutionPlanCache.class);

  private final Path cacheDir;
  private final ObjectMapper mapper = JacksonUtility.getJsonMapper();

  /**
   * Create a new cache. Cache directory is determined from environment variables
   * or falls back to user home directory.
   *
   * @param handbookPath path to the handbook directory (used for fallback only)
   */
  public ExecutionPlanCache(Path handbookPath) {
    this.cacheDir = determineCacheDirectory(handbookPath);
    ensureCacheDirectory();
  }

  /**
   * Determine the cache directory location.
   * Priority: ONEMCP_CACHE_DIR > ONEMCP_HOME_DIR/cache > user home/.onemcp/cache
   */
  private Path determineCacheDirectory(Path handbookPath) {
    // Priority 1: ONEMCP_CACHE_DIR environment variable
    String cacheDirEnv = System.getenv("ONEMCP_CACHE_DIR");
    if (cacheDirEnv != null && !cacheDirEnv.isBlank()) {
      Path cacheDir = java.nio.file.Paths.get(cacheDirEnv);
      log.debug("Using cache directory from ONEMCP_CACHE_DIR: {}", cacheDir);
      return cacheDir;
    }

    // Priority 2: ONEMCP_HOME_DIR/cache
    String homeDir = System.getenv("ONEMCP_HOME_DIR");
    if (homeDir != null && !homeDir.isBlank()) {
      Path cacheDir = java.nio.file.Paths.get(homeDir, "cache");
      log.debug("Using cache directory from ONEMCP_HOME_DIR: {}", cacheDir);
      return cacheDir;
    }

    // Priority 3: User home/.onemcp/cache (fallback)
    String userHome = System.getProperty("user.home");
    if (userHome != null && !userHome.isBlank()) {
      Path cacheDir = java.nio.file.Paths.get(userHome, ".onemcp", "cache");
      log.debug("Using cache directory from user home: {}", cacheDir);
      return cacheDir;
    }

    // Final fallback: handbook/cache (should not happen in normal operation)
    log.warn("Could not determine cache directory, falling back to handbook/cache");
    return handbookPath.resolve("cache");
  }

  /**
   * Ensure the cache directory exists.
   */
  private void ensureCacheDirectory() {
    try {
      if (!Files.exists(cacheDir)) {
        Files.createDirectories(cacheDir);
        log.info("Created cache directory: {}", cacheDir);
      }
    } catch (IOException e) {
      log.warn("Failed to create cache directory: {}", cacheDir, e);
    }
  }

  /**
   * Look up a cached plan with execution metadata.
   *
   * @param schema the PromptSchema (uses its cacheKey)
   * @return Optional containing the plan result with metadata if found, empty otherwise
   */
  public Optional<CachedPlanResult> lookupWithMetadata(PromptSchema schema) {
    if (schema == null || schema.getCacheKey() == null) {
      log.warn("Cannot lookup plan: schema or cacheKey is null");
      return Optional.empty();
    }

    String cacheKey = schema.getCacheKey();
    Path planFile = cacheDir.resolve(cacheKey + ".json");

    if (!Files.exists(planFile)) {
      log.debug("Cache MISS for PSK: {}", cacheKey);
      return Optional.empty();
    }

    try {
      String content = Files.readString(planFile);
      CachedPlanFile cached = mapper.readValue(content, CachedPlanFile.class);
      
      ExecutionPlan plan = ExecutionPlan.fromJson(cached.planJson);
      
      // Parse execution metadata (defaults for backward compatibility)
      boolean executionSucceeded = "SUCCESS".equals(cached.executionStatus) || 
          (cached.executionStatus == null && cached.failedAttempts == null); // Old format = assume success
      String lastError = cached.lastError;
      int failedAttempts = cached.failedAttempts != null ? cached.failedAttempts : 0;
      
      if (executionSucceeded) {
        log.info("Cache HIT for PSK: {} (cached at {})", cacheKey, cached.createdAt);
      } else {
        log.info("Cache HIT for PSK: {} (cached at {}, FAILED - {} attempts)", 
            cacheKey, cached.createdAt, failedAttempts);
      }
      
      return Optional.of(new CachedPlanResult(plan, executionSucceeded, lastError, failedAttempts));
    } catch (Exception e) {
      log.error("Failed to load cached plan for PSK: {}", cacheKey, e);
      // Delete corrupted cache file
      try {
        Files.deleteIfExists(planFile);
        log.info("Deleted corrupted cache file: {}", planFile);
      } catch (IOException ioe) {
        log.warn("Failed to delete corrupted cache file: {}", planFile, ioe);
      }
      return Optional.empty();
    }
  }

  /**
   * Store a plan in the cache.
   *
   * @param schema the PromptSchema (uses its cacheKey)
   * @param plan the ExecutionPlan to cache
   */
  public void store(PromptSchema schema, ExecutionPlan plan) {
    store(schema, plan, true, null);
  }

  /**
   * Store a plan in the cache with execution status.
   *
   * @param schema the PromptSchema (uses its cacheKey)
   * @param plan the ExecutionPlan to cache
   * @param executionSucceeded whether execution succeeded
   * @param errorMessage error message if execution failed (null if succeeded)
   */
  public void store(PromptSchema schema, ExecutionPlan plan, boolean executionSucceeded, String errorMessage) {
    if (schema == null || schema.getCacheKey() == null) {
      log.warn("Cannot store plan: schema or cacheKey is null");
      return;
    }
    if (plan == null) {
      log.warn("Cannot store plan: plan is null");
      return;
    }

    String cacheKey = schema.getCacheKey();
    Path planFile = cacheDir.resolve(cacheKey + ".json");

    try {
      ensureCacheDirectory();

      // Load existing metadata to preserve failedAttempts count
      int failedAttempts = 0;
      if (Files.exists(planFile)) {
        try {
          String existingContent = Files.readString(planFile);
          CachedPlanFile existing = mapper.readValue(existingContent, CachedPlanFile.class);
          if (existing.failedAttempts != null) {
            failedAttempts = existing.failedAttempts;
          }
        } catch (Exception e) {
          log.debug("Could not read existing cache metadata, starting fresh", e);
        }
      }

      // Increment failed attempts if execution failed
      if (!executionSucceeded) {
        failedAttempts++;
      } else {
        // Reset on success
        failedAttempts = 0;
      }

      CachedPlanFile cached = new CachedPlanFile();
      cached.psk = cacheKey;
      cached.createdAt = Instant.now().toString();
      cached.planJson = plan.toJson();
      cached.executionStatus = executionSucceeded ? "SUCCESS" : "FAILED";
      cached.lastError = errorMessage;
      cached.failedAttempts = failedAttempts;

      String content = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cached);
      Files.writeString(planFile, content);
      
      if (executionSucceeded) {
        log.info("Stored plan in cache: {} (PSK: {})", planFile.getFileName(), cacheKey);
      } else {
        log.info("Stored failed plan in cache: {} (PSK: {}, attempts: {})", 
            planFile.getFileName(), cacheKey, failedAttempts);
      }
    } catch (Exception e) {
      log.error("Failed to store plan for PSK: {}", cacheKey, e);
    }
  }

  /**
   * Check if a plan exists in the cache.
   *
   * @param schema the PromptSchema
   * @return true if a cached plan exists
   */
  public boolean exists(PromptSchema schema) {
    if (schema == null || schema.getCacheKey() == null) {
      return false;
    }
    Path planFile = cacheDir.resolve(schema.getCacheKey() + ".json");
    return Files.exists(planFile);
  }

  /**
   * Remove a cached plan.
   *
   * @param schema the PromptSchema
   * @return true if the plan was removed
   */
  public boolean remove(PromptSchema schema) {
    if (schema == null || schema.getCacheKey() == null) {
      return false;
    }
    return removeByCacheKey(schema.getCacheKey());
  }

  /**
   * Remove a cached plan by cache key.
   *
   * @param cacheKey the cache key (PSK)
   * @return true if the plan was removed
   */
  public boolean removeByCacheKey(String cacheKey) {
    if (cacheKey == null || cacheKey.isEmpty()) {
      return false;
    }
    Path planFile = cacheDir.resolve(cacheKey + ".json");
    try {
      boolean deleted = Files.deleteIfExists(planFile);
      if (deleted) {
        log.info("Removed cached plan for PSK: {}", cacheKey);
      }
      return deleted;
    } catch (IOException e) {
      log.error("Failed to remove cached plan for PSK: {}", cacheKey, e);
      return false;
    }
  }

  /**
   * Clear all cached plans.
   *
   * @return the number of plans removed
   */
  public int clear() {
    int count = 0;
    try {
      if (Files.exists(cacheDir)) {
        try (var stream = Files.list(cacheDir)) {
          var files = stream.filter(p -> p.toString().endsWith(".json")).toList();
          for (Path file : files) {
            try {
              Files.delete(file);
              count++;
            } catch (IOException e) {
              log.warn("Failed to delete cache file: {}", file, e);
            }
          }
        }
      }
      log.info("Cleared {} cached plan(s)", count);
    } catch (IOException e) {
      log.error("Failed to clear cache", e);
    }
    return count;
  }

  /**
   * Get the cache directory path.
   *
   * @return the cache directory
   */
  public Path getCacheDirectory() {
    return cacheDir;
  }

  /**
   * Export all cached plans to a target directory.
   *
   * @param targetDir the directory to export cache files to
   * @return the number of plans exported
   */
  public int exportTo(Path targetDir) throws IOException {
    if (!Files.exists(cacheDir)) {
      log.warn("Cache directory does not exist: {}", cacheDir);
      return 0;
    }

    // Ensure target directory exists
    Files.createDirectories(targetDir);

    int count = 0;
    try (var stream = Files.list(cacheDir)) {
      var files = stream.filter(p -> p.toString().endsWith(".json")).toList();
      for (Path sourceFile : files) {
        Path targetFile = targetDir.resolve(sourceFile.getFileName());
        Files.copy(sourceFile, targetFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        count++;
        log.debug("Exported cache file: {} -> {}", sourceFile.getFileName(), targetFile);
      }
    }
    log.info("Exported {} cached plan(s) to {}", count, targetDir);
    return count;
  }

  /**
   * Import cached plans from a source directory.
   *
   * @param sourceDir the directory to import cache files from
   * @return the number of plans imported
   */
  public int importFrom(Path sourceDir) throws IOException {
    if (!Files.exists(sourceDir)) {
      log.warn("Source directory does not exist: {}", sourceDir);
      return 0;
    }

    // Ensure cache directory exists
    ensureCacheDirectory();

    int count = 0;
    try (var stream = Files.list(sourceDir)) {
      var files = stream.filter(p -> p.toString().endsWith(".json")).toList();
      for (Path sourceFile : files) {
        Path targetFile = cacheDir.resolve(sourceFile.getFileName());
        Files.copy(sourceFile, targetFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        count++;
        log.debug("Imported cache file: {} -> {}", sourceFile.getFileName(), targetFile);
      }
    }
    log.info("Imported {} cached plan(s) from {}", count, sourceDir);
    return count;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Internal: Cached plan file structure
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Result of a cache lookup, including the plan and execution metadata.
   */
  public static class CachedPlanResult {
    public final ExecutionPlan plan;
    public final boolean executionSucceeded;
    public final String lastError;
    public final int failedAttempts;

    public CachedPlanResult(ExecutionPlan plan, boolean executionSucceeded, String lastError, int failedAttempts) {
      this.plan = plan;
      this.executionSucceeded = executionSucceeded;
      this.lastError = lastError;
      this.failedAttempts = failedAttempts;
    }
  }

  /**
   * Structure of a cached plan file.
   */
  static class CachedPlanFile {
    @JsonProperty("psk")
    String psk;

    @JsonProperty("created_at")
    String createdAt;

    @JsonProperty("plan")
    String planJson;

    @JsonProperty("execution_status")
    String executionStatus; // "SUCCESS" or "FAILED"

    @JsonProperty("last_error")
    String lastError;

    @JsonProperty("failed_attempts")
    Integer failedAttempts; // Number of times execution failed
  }
}


