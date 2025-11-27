package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

/**
 * Cache for execution plans, stored as JSON files in the handbook's plans directory.
 *
 * <p>Each cached plan is stored in a file named {@code <cache_key>.json} where
 * the cache key is derived from the {@link PromptSchema}.
 *
 * <p>The cache treats {@link ExecutionPlan} as opaque - it doesn't inspect the
 * plan's contents, just serializes/deserializes it.
 */
public class ExecutionPlanCache {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ExecutionPlanCache.class);

  private final Path cacheDir;
  private final ObjectMapper mapper = JacksonUtility.getJsonMapper();

  /**
   * Create a new cache for the given handbook.
   *
   * @param handbookPath path to the handbook directory
   */
  public ExecutionPlanCache(Path handbookPath) {
    this.cacheDir = handbookPath.resolve("plans");
    ensureCacheDirectory();
  }

  /**
   * Ensure the cache directory exists.
   */
  private void ensureCacheDirectory() {
    try {
      if (!Files.exists(cacheDir)) {
        Files.createDirectories(cacheDir);
        log.info("Created plan cache directory: {}", cacheDir);
      }
    } catch (IOException e) {
      log.warn("Failed to create cache directory: {}", cacheDir, e);
    }
  }

  /**
   * Look up a cached plan by its schema.
   *
   * @param schema the PromptSchema (uses its cacheKey)
   * @return Optional containing the plan if found, empty otherwise
   */
  public Optional<ExecutionPlan> lookup(PromptSchema schema) {
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
      log.info("Cache HIT for PSK: {} (cached at {})", cacheKey, cached.createdAt);
      return Optional.of(plan);
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

      CachedPlanFile cached = new CachedPlanFile();
      cached.psk = cacheKey;
      cached.createdAt = Instant.now().toString();
      cached.planJson = plan.toJson();

      String content = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cached);
      Files.writeString(planFile, content);
      
      log.info("Stored plan in cache: {} (PSK: {})", planFile.getFileName(), cacheKey);
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
    Path planFile = cacheDir.resolve(schema.getCacheKey() + ".json");
    try {
      boolean deleted = Files.deleteIfExists(planFile);
      if (deleted) {
        log.info("Removed cached plan for PSK: {}", schema.getCacheKey());
      }
      return deleted;
    } catch (IOException e) {
      log.error("Failed to remove cached plan for PSK: {}", schema.getCacheKey(), e);
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

  // ─────────────────────────────────────────────────────────────────────────
  // Internal: Cached plan file structure
  // ─────────────────────────────────────────────────────────────────────────

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
  }
}


