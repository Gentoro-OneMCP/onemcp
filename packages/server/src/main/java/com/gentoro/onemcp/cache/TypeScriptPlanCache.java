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
 * Cache for TypeScript execution plans, stored as .ts files in cache/plans directory.
 *
 * <p>Each cached plan is stored in a file named {@code <cache_key>.ts} where
 * the cache key is derived from the canonical S-SQL.
 *
 * <p>Cache location priority:
 * <ol>
 *   <li>ONEMCP_CACHE_DIR/plans (if ONEMCP_CACHE_DIR is set)
 *   <li>ONEMCP_HOME_DIR/cache/plans (if ONEMCP_HOME_DIR is set)
 *   <li>User home/.onemcp/cache/plans (fallback)
 * </ol>
 */
public class TypeScriptPlanCache {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(TypeScriptPlanCache.class);

  private final Path plansDir;
  private final ObjectMapper mapper = JacksonUtility.getJsonMapper();

  /**
   * Create a new cache. Plans directory is determined from environment variables
   * or falls back to user home directory.
   *
   * @param cacheDir the base cache directory (e.g., from ExecutionPlanCache)
   */
  public TypeScriptPlanCache(Path cacheDir) {
    this.plansDir = cacheDir.resolve("plans");
    ensurePlansDirectory();
  }

  /**
   * Ensure the plans directory exists.
   */
  private void ensurePlansDirectory() {
    try {
      if (!Files.exists(plansDir)) {
        Files.createDirectories(plansDir);
        log.info("Created plans directory: {}", plansDir);
      }
    } catch (IOException e) {
      log.warn("Failed to create plans directory: {}", plansDir, e);
    }
  }

  /**
   * Look up a cached TypeScript plan by cache key.
   *
   * @param cacheKey the cache key (e.g., MD5 hash of canonical SQL)
   * @return Optional containing the cached plan result with metadata if found, empty otherwise
   */
  public Optional<CachedPlanResult> lookupByCacheKey(String cacheKey) {
    if (cacheKey == null || cacheKey.isEmpty()) {
      log.warn("Cannot lookup plan: cacheKey is null or empty");
      return Optional.empty();
    }
    
    Path planFile = plansDir.resolve(cacheKey + ".ts");
    Path metadataFile = plansDir.resolve(cacheKey + ".json");

    if (!Files.exists(planFile)) {
      log.debug("Cache MISS for cache key: {}", cacheKey);
      return Optional.empty();
    }

    try {
      String typescriptCode = Files.readString(planFile);
      
      // Clean cached code: Remove "javascript" if it appears as the first line
      // This fixes cached code that was generated before the extraction fix
      typescriptCode = typescriptCode.trim();
      if (typescriptCode.startsWith("javascript\r\n")) {
        typescriptCode = typescriptCode.substring(13).trim(); // Remove "javascript\r\n" (13 chars)
        log.debug("Cleaned 'javascript' prefix from cached TypeScript code");
      } else if (typescriptCode.startsWith("javascript\n")) {
        typescriptCode = typescriptCode.substring(11).trim(); // Remove "javascript\n" (11 chars)
        log.debug("Cleaned 'javascript' prefix from cached TypeScript code");
      } else if (typescriptCode.equals("javascript")) {
        typescriptCode = ""; // Edge case: code is just "javascript"
        log.warn("Cached TypeScript code was just 'javascript', clearing it");
      }
      
      // Load metadata if available
      boolean executionSucceeded = true;
      String lastError = null;
      int failedAttempts = 0;
      String normalizationNote = null;
      boolean normalizationFailed = false;
      
      if (Files.exists(metadataFile)) {
        try {
          String metadataContent = Files.readString(metadataFile);
          CachedPlanMetadata metadata = mapper.readValue(metadataContent, CachedPlanMetadata.class);
          executionSucceeded = "SUCCESS".equals(metadata.executionStatus) || 
              (metadata.executionStatus == null && metadata.failedAttempts == null);
          lastError = metadata.lastError;
          failedAttempts = metadata.failedAttempts != null ? metadata.failedAttempts : 0;
          normalizationNote = metadata.normalizationNote;
          normalizationFailed = metadata.normalizationFailed != null && metadata.normalizationFailed;
        } catch (Exception e) {
          log.debug("Could not read metadata file, using defaults", e);
        }
      }
      
      if (executionSucceeded) {
        log.info("Cache HIT for cache key: {} (TypeScript plan)", cacheKey);
      } else {
        log.info("Cache HIT for cache key: {} (TypeScript plan, FAILED - {} attempts)", 
            cacheKey, failedAttempts);
      }
      if (normalizationFailed) {
        log.info("Cache HIT for cache key: {} (normalization failed: {})", cacheKey, normalizationNote);
      }
      
      return Optional.of(new CachedPlanResult(typescriptCode, executionSucceeded, lastError, 
          failedAttempts, normalizationNote, normalizationFailed));
    } catch (Exception e) {
      log.error("Failed to load cached TypeScript plan for cache key: {}", cacheKey, e);
      // Delete corrupted cache files
      try {
        Files.deleteIfExists(planFile);
        Files.deleteIfExists(metadataFile);
        log.info("Deleted corrupted cache files for: {}", cacheKey);
      } catch (IOException ioe) {
        log.warn("Failed to delete corrupted cache files", ioe);
      }
      return Optional.empty();
    }
  }

  /**
   * Store a TypeScript plan in the cache.
   *
   * @param cacheKey the cache key (e.g., MD5 hash of canonical SQL)
   * @param typescriptCode the TypeScript code to cache
   * @param executionSucceeded whether execution succeeded
   * @param errorMessage error message if execution failed (null if succeeded)
   */
  public void storeByCacheKey(String cacheKey, String typescriptCode, 
      boolean executionSucceeded, String errorMessage) {
    storeByCacheKey(cacheKey, typescriptCode, executionSucceeded, errorMessage, null, false);
  }

  /**
   * Store a TypeScript plan in the cache with normalization metadata.
   *
   * @param cacheKey the cache key (e.g., MD5 hash of canonical SQL)
   * @param typescriptCode the TypeScript code to cache
   * @param executionSucceeded whether execution succeeded
   * @param errorMessage error message if execution failed (null if succeeded)
   * @param normalizationNote note from PromptSchema (explanation/caveat)
   * @param normalizationFailed whether normalization failed (operation is null)
   */
  public void storeByCacheKey(String cacheKey, String typescriptCode, 
      boolean executionSucceeded, String errorMessage, String normalizationNote, boolean normalizationFailed) {
    if (cacheKey == null || cacheKey.isEmpty()) {
      log.warn("Cannot store plan: cacheKey is null or empty");
      return;
    }
    if (typescriptCode == null || typescriptCode.trim().isEmpty()) {
      log.warn("Cannot store plan: typescriptCode is null or empty");
      return;
    }
    
    Path planFile = plansDir.resolve(cacheKey + ".ts");
    Path metadataFile = plansDir.resolve(cacheKey + ".json");

    try {
      ensurePlansDirectory();

      // Load existing metadata to preserve failedAttempts count
      int failedAttempts = 0;
      if (Files.exists(metadataFile)) {
        try {
          String existingContent = Files.readString(metadataFile);
          CachedPlanMetadata existing = mapper.readValue(existingContent, CachedPlanMetadata.class);
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

      // Write TypeScript file
      Files.writeString(planFile, typescriptCode);
      
      // Write metadata file
      CachedPlanMetadata metadata = new CachedPlanMetadata();
      metadata.cacheKey = cacheKey;
      metadata.createdAt = Instant.now().toString();
      metadata.executionStatus = executionSucceeded ? "SUCCESS" : "FAILED";
      metadata.lastError = errorMessage;
      metadata.failedAttempts = failedAttempts;
      metadata.normalizationNote = normalizationNote;
      metadata.normalizationFailed = normalizationFailed;
      
      String metadataContent = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);
      Files.writeString(metadataFile, metadataContent);
      
      if (executionSucceeded) {
        log.info("Stored TypeScript plan in cache: {} (PSK: {})", planFile.getFileName(), cacheKey);
      } else {
        log.info("Stored failed TypeScript plan in cache: {} (PSK: {}, attempts: {})", 
            planFile.getFileName(), cacheKey, failedAttempts);
      }
    } catch (Exception e) {
      log.error("Failed to store TypeScript plan for PSK: {}", cacheKey, e);
    }
  }

  /**
   * Get the plans directory path.
   *
   * @return the plans directory
   */
  public Path getPlansDirectory() {
    return plansDir;
  }

  /**
   * Result of a cache lookup, including the TypeScript code and execution metadata.
   */
  public static class CachedPlanResult {
    public final String typescriptCode;
    public final boolean executionSucceeded;
    public final String lastError;
    public final int failedAttempts;
    public final String normalizationNote; // Note from PromptSchema (explanation/caveat)
    public final boolean normalizationFailed; // Whether normalization failed (operation is null)

    public CachedPlanResult(String typescriptCode, boolean executionSucceeded, 
        String lastError, int failedAttempts) {
      this(typescriptCode, executionSucceeded, lastError, failedAttempts, null, false);
    }

    public CachedPlanResult(String typescriptCode, boolean executionSucceeded, 
        String lastError, int failedAttempts, String normalizationNote, boolean normalizationFailed) {
      this.typescriptCode = typescriptCode;
      this.executionSucceeded = executionSucceeded;
      this.lastError = lastError;
      this.failedAttempts = failedAttempts;
      this.normalizationNote = normalizationNote;
      this.normalizationFailed = normalizationFailed;
    }
  }

  /**
   * Structure of a cached plan metadata file.
   */
  static class CachedPlanMetadata {
    @JsonProperty("cache_key")
    String cacheKey;

    @JsonProperty("created_at")
    String createdAt;

    @JsonProperty("execution_status")
    String executionStatus; // "SUCCESS" or "FAILED"

    @JsonProperty("last_error")
    String lastError;

    @JsonProperty("failed_attempts")
    Integer failedAttempts; // Number of times execution failed

    @JsonProperty("normalization_note")
    String normalizationNote; // Note from PromptSchema (explanation/caveat)

    @JsonProperty("normalization_failed")
    Boolean normalizationFailed; // Whether normalization failed (operation is null)
  }
}

