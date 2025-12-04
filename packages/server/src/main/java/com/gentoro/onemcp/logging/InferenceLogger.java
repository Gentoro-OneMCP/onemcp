package com.gentoro.onemcp.logging;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.messages.AssigmentResult;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;

/**
 * Central component for logging inference execution details.
 *
 * <p>Operates in two modes:
 *
 * <ul>
 *   <li><b>Report Mode (Enabled)</b>: Generates detailed execution reports with full information
 *       including LLM prompts, inputs, outputs, generated code, API calls, etc.
 *   <li><b>Normal Logging Mode (Disabled)</b>: Emits production-safe logging entries that exclude
 *       verbose information like generated code, LLM prompts, and detailed inputs/outputs.
 * </ul>
 *
 * <p>Report mode is automatically enabled for CLI/handbook usage and can be controlled via:
 *
 * <ul>
 *   <li>Environment variable: {@code ONEMCP_REPORTS_ENABLED}
 *   <li>Config file: {@code reports.enabled}
 *   <li>Auto-detection: Enabled if handbook path exists (CLI mode)
 * </ul>
 */
public class InferenceLogger {
  private static final Logger log = LoggingService.getLogger(InferenceLogger.class);

  private final OneMcp oneMcp;
  private final boolean reportModeEnabled;
  private final Path reportsDirectory;

  // In-memory event storage: executionId -> list of events
  private final Map<String, List<ExecutionEvent>> executionEvents = new ConcurrentHashMap<>();

  // Track report paths per execution (kept until retrieved)
  private final Map<String, String> executionReportPaths = new ConcurrentHashMap<>();

  // Thread-local execution ID for tracking context across async operations
  private static final ThreadLocal<String> currentExecutionId = new ThreadLocal<>();

  // Thread-local report path for current execution
  private static final ThreadLocal<String> currentReportPath = new ThreadLocal<>();

  public InferenceLogger(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    this.reportModeEnabled = detectReportMode();
    this.reportsDirectory = determineReportsDirectory();

    if (reportModeEnabled) {
      log.info("Report mode enabled - detailed execution reports will be generated");
      log.info("Reports directory: {}", reportsDirectory);
    } else {
      log.debug("Report mode disabled - only production-safe logs will be emitted");
    }
  }

  /**
   * Detect if report mode should be enabled.
   *
   * <p>Priority:
   *
   * <ol>
   *   <li>Environment variable {@code ONEMCP_REPORTS_ENABLED}
   *   <li>Config file {@code reports.enabled}
   *   <li>Auto-enable if handbook path exists (CLI mode)
   *   <li>Default: disabled (production mode)
   * </ol>
   */
  private boolean detectReportMode() {
    // Check environment variable
    String envReportMode = System.getenv("ONEMCP_REPORTS_ENABLED");
    if (envReportMode != null && !envReportMode.isBlank()) {
      return "true".equalsIgnoreCase(envReportMode.trim());
    }

    // Check config file
    Configuration config = oneMcp.configuration();
    if (config != null) {
      String configReportMode = config.getString("reports.enabled", null);
      if (configReportMode != null && !configReportMode.isBlank()) {
        return "true".equalsIgnoreCase(configReportMode.trim());
      }
    }

    // Auto-enable for handbook mode (CLI usage)
    try {
      Handbook handbook = oneMcp.handbook();
      if (handbook != null) {
        Path handbookPath = handbook.location();
        if (handbookPath != null && Files.exists(handbookPath)) {
          log.info("Report mode auto-enabled for CLI/handbook mode");
          return true;
        }
      }
    } catch (Exception e) {
      log.debug("Could not auto-detect handbook mode: {}", e.getMessage());
    }

    // Production mode: disable by default
    return false;
  }

  /**
   * Determine the logging directory for reports.
   *
   * <p>Priority:
   *
   * <ol>
   *   <li>Environment variable {@code ONEMCP_LOG_DIR} (CLI mode: set to {@code ONEMCP_HOME_DIR/logs})
   *   <li>Config file {@code logging.directory}
   *   <li>If handbook mode detected: use {@code {handbook}/logs/} (fallback for CLI mode)
   *   <li>Default: {@code /var/log/onemcp} (production mode)
   * </ol>
   *
   * <p>Behavior:
   *
   * <ul>
   *   <li><b>CLI mode</b>: CLI sets {@code ONEMCP_LOG_DIR} to {@code ONEMCP_HOME_DIR/logs}, so reports
   *       go to {@code ONEMCP_HOME_DIR/logs/reports/}
   *   <li><b>Production mode</b>: When {@code ONEMCP_LOG_DIR} is not set and no handbook is
   *       detected, defaults to {@code /var/log/onemcp/reports/}
   * </ul>
   *
   * <p>Reports are always stored in {@code {logging_dir}/reports/} subdirectory.
   */
  private Path determineReportsDirectory() {
    Path baseLogDir;

    // Priority 1: Environment variable (CLI mode sets ONEMCP_LOG_DIR to ONEMCP_HOME_DIR/logs)
    String envLogDir = System.getenv("ONEMCP_LOG_DIR");
    if (envLogDir != null && !envLogDir.isBlank()) {
      baseLogDir = Paths.get(envLogDir);
      log.debug("Using logging directory from ONEMCP_LOG_DIR: {}", baseLogDir);
    } else {
      // Priority 2: Try ONEMCP_HOME_DIR/logs (CLI mode fallback)
      String homeDir = System.getenv("ONEMCP_HOME_DIR");
      if (homeDir != null && !homeDir.isBlank()) {
        baseLogDir = Paths.get(homeDir, "logs");
        log.debug("Using logging directory from ONEMCP_HOME_DIR: {}", baseLogDir);
      } else {
        // Priority 3: Config file
        Configuration config = oneMcp.configuration();
        String configLogDir = config != null ? config.getString("logging.directory", null) : null;
        if (configLogDir != null && !configLogDir.isBlank()) {
          baseLogDir = Paths.get(configLogDir);
          log.debug("Using logging directory from config: {}", baseLogDir);
        } else {
          // Priority 4: Default to production mode location (never use handbook/logs)
          baseLogDir = Paths.get("/var/log/onemcp");
          log.debug("Using default production logging directory: {}", baseLogDir);
        }
      }
    }

    // Safety check: Never create logs in handbook directory
    // If baseLogDir is within the handbook directory, redirect to ONEMCP_HOME_DIR/logs
    try {
      Handbook handbook = oneMcp.handbook();
      if (handbook != null) {
        Path handbookPath = handbook.location();
        if (handbookPath != null && Files.exists(handbookPath)) {
          Path normalizedHandbook = handbookPath.normalize().toAbsolutePath();
          Path normalizedLogDir = baseLogDir.normalize().toAbsolutePath();
          
          // Check if log directory is within or equal to handbook directory
          if (normalizedLogDir.startsWith(normalizedHandbook) || normalizedLogDir.equals(normalizedHandbook)) {
            log.warn(
                "Detected attempt to create logs in handbook directory ({}), redirecting to ONEMCP_HOME_DIR/logs",
                baseLogDir);
            // Redirect to ONEMCP_HOME_DIR/logs
            String homeDir = System.getenv("ONEMCP_HOME_DIR");
            if (homeDir != null && !homeDir.isBlank()) {
              baseLogDir = Paths.get(homeDir, "logs");
            } else {
              baseLogDir = Paths.get("/var/log/onemcp");
            }
            log.info("Using redirected logging directory: {}", baseLogDir);
          }
        }
      }
    } catch (Exception e) {
      log.debug("Could not check handbook path for log directory safety: {}", e.getMessage());
    }

    // Reports go in {baseLogDir}/reports/
    Path reportsDir = baseLogDir.resolve("reports");

    // FINAL safety check: Never create reports in handbook directory
    // This check runs AFTER all other logic to catch any edge cases
    try {
      Handbook handbook = oneMcp.handbook();
      if (handbook != null) {
        Path handbookPath = handbook.location();
        if (handbookPath != null && Files.exists(handbookPath)) {
          Path normalizedHandbook = handbookPath.normalize().toAbsolutePath();
          Path normalizedReports = reportsDir.normalize().toAbsolutePath();
          
          // Check if reports directory is within or equal to handbook directory
          if (normalizedReports.startsWith(normalizedHandbook) || normalizedReports.equals(normalizedHandbook)) {
            log.error(
                "CRITICAL: Reports directory ({}) is within handbook directory ({}). Redirecting immediately!",
                reportsDir,
                handbookPath);
            // Force redirect to ONEMCP_HOME_DIR/logs/reports
            String homeDir = System.getenv("ONEMCP_HOME_DIR");
            if (homeDir != null && !homeDir.isBlank()) {
              reportsDir = Paths.get(homeDir, "logs", "reports");
            } else {
              reportsDir = Paths.get("/var/log/onemcp/reports");
            }
            log.warn("CRITICAL: Redirected reports directory to: {}", reportsDir);
          }
        }
      }
    } catch (Exception e) {
      log.debug("Could not perform final reports directory safety check: {}", e.getMessage());
    }

    // Ensure directory exists
    try {
      Files.createDirectories(reportsDir);
    } catch (IOException e) {
      log.warn("Failed to create reports directory: {}", reportsDir, e);
    }

    return reportsDir;
  }

  /**
   * Get the handbook location for display in reports.
   *
   * @return handbook path as string, or null if not available
   */
  private String getHandbookLocation() {
    // Priority 1: HANDBOOK_DIR environment variable (set by CLI)
    String envHandbookDir = System.getenv("HANDBOOK_DIR");
    if (envHandbookDir != null && !envHandbookDir.isBlank()) {
      Path envHandbookPath = Paths.get(envHandbookDir);
      if (Files.exists(envHandbookPath) && Files.isDirectory(envHandbookPath)) {
        return envHandbookPath.toString();
      }
    }

    // Priority 2: KnowledgeBase handbook path
    try {
      if (oneMcp.handbook() != null) {
        if (oneMcp.handbook().location() != null) {
          return oneMcp.handbook().location().toString();
        }
      }
    } catch (Exception e) {
      log.debug("Could not get handbook path from KnowledgeBase: {}", e.getMessage());
    }

    return null;
  }

  /**
   * Get the current execution ID from thread-local.
   * 
   * @return current execution ID, or null if not set
   */
  public String getCurrentExecutionId() {
    return currentExecutionId.get();
  }

  /**
   * Start tracking an execution.
   *
   * @param executionId unique execution identifier
   * @param userQuery the user's query/prompt
   * @return the report path that will be used (or null if report mode disabled)
   */
  public String startExecution(String executionId, String userQuery) {
    currentExecutionId.set(executionId);

    if (reportModeEnabled) {
      // Generate report path pre-operation
      String timestamp =
          Instant.now()
              .atOffset(ZoneOffset.UTC)
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSSSSS'Z'"));
      String filename = "execution-" + timestamp + ".txt";
      Path reportPath = reportsDirectory.resolve(filename);
      executionReportPaths.put(executionId, reportPath.toString());

      // Initialize event storage
      List<ExecutionEvent> events = new ArrayList<>();
      events.add(
          new ExecutionEvent(
              "execution_started",
              executionId,
              Instant.now().toString(),
              Map.of("userQuery", userQuery)));
      executionEvents.put(executionId, events);

      log.info("Execution started: {} (report: {})", executionId, reportPath);
      return reportPath.toString();
    } else {
      log.info("Execution started: {}", executionId);
      return null;
    }
  }

  /**
   * Complete an execution and generate the report.
   *
   * @param executionId unique execution identifier
   * @param durationMs execution duration in milliseconds
   * @param success whether execution succeeded
   * @return the report path (or null if report mode disabled or no report generated)
   */
  public String completeExecution(String executionId, long durationMs, boolean success) {
    return completeExecution(executionId, durationMs, success, 0);
  }

  /**
   * Complete an execution and generate the report.
   *
   * @param executionId unique execution identifier
   * @param durationMs execution duration in milliseconds
   * @param success whether execution succeeded
   * @param startTimeMs execution start time in milliseconds (for log capture)
   * @return the report path (or null if report mode disabled or no report generated)
   */
  public String completeExecution(String executionId, long durationMs, boolean success, long startTimeMs) {
    try {
      if (reportModeEnabled) {
        List<ExecutionEvent> events = executionEvents.get(executionId);
        String reportPathStr = executionReportPaths.get(executionId);

        if (events != null && reportPathStr != null) {
          Path reportPath = Paths.get(reportPathStr);
          
          // Add completion event
          events.add(
              new ExecutionEvent(
                  "execution_complete",
                  executionId,
                  Instant.now().toString(),
                  Map.of("durationMs", durationMs, "success", success)));

          // Generate and write report
          String reportContent = generateTextReport(executionId, events, durationMs, success, startTimeMs);
          Files.writeString(reportPath, reportContent);
          log.debug("Report written to: {} (size: {} bytes)", reportPath, reportContent.length());

          currentReportPath.set(reportPathStr);
          log.info("Execution completed: {} (report: {})", executionId, reportPath);

          // Clean up events, but keep executionReportPaths until it's retrieved
          executionEvents.remove(executionId);
          currentExecutionId.remove();
          // Note: executionReportPaths and currentReportPath are NOT removed here - 
          // they will be removed when retrieved via getCurrentReportPath()

          return reportPathStr;
        }
      } else {
        log.info("Execution completed: {} ({}ms, success: {})", executionId, durationMs, success);
      }
    } catch (Exception e) {
      log.error("Failed to generate report for execution: {}", executionId, e);
    } finally {
      // Clean up execution tracking, but preserve report path for retrieval
      executionEvents.remove(executionId);
      // executionReportPaths is intentionally NOT removed here - it will be cleared after retrieval
      currentExecutionId.remove();
      // currentReportPath is intentionally NOT removed here - it will be cleared after retrieval
    }

    return null;
  }

  /**
   * Set the execution ID in thread-local for background threads. This allows background threads to
   * log events to the same execution.
   *
   * @param executionId the execution ID to set
   */
  public void setExecutionId(String executionId) {
    if (executionId != null) {
      currentExecutionId.set(executionId);
    }
  }

  /**
   * Get the report path for the most recent execution. 
   * 
   * <p>This method looks up the report path from the executionReportPaths map.
   * It returns the most recently completed execution's report path and removes it
   * from the map to prevent memory leaks.
   *
   * @return report path or null if not available
   */
  public String getCurrentReportPath() {
    // First try thread-local (for same-thread access)
    String path = currentReportPath.get();
    if (path != null) {
      currentReportPath.remove();
      // Also remove from map if present
      executionReportPaths.values().remove(path);
      return path;
    }
    
    // If thread-local is empty (different thread), get the most recent one from map
    // This handles the case where completeExecution was called in a different thread
    if (!executionReportPaths.isEmpty()) {
      // Get the most recent entry (last one added)
      // Since ConcurrentHashMap doesn't preserve insertion order, we'll get any entry
      // In practice, there should only be one entry at a time
      String mostRecentPath = executionReportPaths.values().iterator().next();
      // Remove all entries (should only be one)
      executionReportPaths.clear();
      return mostRecentPath;
    }
    
    return null;
  }

  /**
   * Generate a formatted text report from execution events.
   *
   * @param executionId execution identifier
   * @param events list of execution events
   * @param durationMs total duration
   * @param success whether execution succeeded
   * @return formatted report text
   */
  private String generateTextReport(
      String executionId, List<ExecutionEvent> events, long durationMs, boolean success) {
    return generateTextReport(executionId, events, durationMs, success, 0);
  }

  private String generateTextReport(
      String executionId, List<ExecutionEvent> events, long durationMs, boolean success, long startTimeMs) {
    StringBuilder sb = new StringBuilder();

    // Header
    sb.append("╔══════════════════════════════════════════════════════════════════════════════╗\n");
    sb.append("║                          EXECUTION REPORT                                    ║\n");
    sb.append("╚══════════════════════════════════════════════════════════════════════════════╝\n");
    sb.append("\n");

    // Find start event for timestamp
    String startTimestamp =
        events.stream()
            .filter(e -> "execution_started".equals(e.type))
            .findFirst()
            .map(e -> e.timestamp)
            .orElse(Instant.now().toString());

    sb.append("  Timestamp: ").append(startTimestamp).append("\n");
    sb.append("  Duration:  ")
        .append(durationMs)
        .append("ms (")
        .append(durationMs / 1000.0)
        .append("s)\n");

    // Handbook location
    try {
      String handbookLocation = getHandbookLocation();
      if (handbookLocation != null && !handbookLocation.isBlank()) {
        sb.append("  Handbook:  ").append(handbookLocation).append("\n");
      }
    } catch (Exception e) {
      log.debug("Could not determine handbook location for report: {}", e.getMessage());
    }

    sb.append("\n");

    // Combined Execution Summary
    long apiCalls = events.stream().filter(e -> "api_call".equals(e.type)).count();
    long errors = events.stream().filter(e -> "api_call_error".equals(e.type)).count();
    
    // Check execution plan cache status (preferred over LLM cache status)
    Boolean planCacheHit = null;
    int planEventCount = 0;
    for (ExecutionEvent event : events) {
      if ("execution_plan".equals(event.type)) {
        planEventCount++;
        Object planCacheHitObj = event.data != null ? event.data.get("planCacheHit") : null;
        log.debug("Found execution_plan event #{}: planCacheHit={}", planEventCount, planCacheHitObj);
        if (planCacheHitObj instanceof Boolean) {
          planCacheHit = (Boolean) planCacheHitObj;
          log.debug("Using planCacheHit={} from execution_plan event", planCacheHit);
          break; // Use first plan's cache status
        }
      }
    }
    if (planEventCount == 0) {
      log.debug("No execution_plan events found in events list");
    } else if (planCacheHit == null) {
      log.debug("Found {} execution_plan event(s) but none had planCacheHit set", planEventCount);
    }

    // Determine cache status for header
    // Only show cache status if plan caching is enabled (planCacheHit is not null)
    // When planCacheHit is null, caching is disabled, so don't show any cache status
    String cacheStatus = "";
    if (planCacheHit != null) {
      // Plan caching is enabled - show cache status
      cacheStatus = planCacheHit ? " - Cache hit" : " - Cache miss";
    }
    // If planCacheHit is null, caching is disabled - don't show cache status

    sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    String headerText = "EXECUTION SUMMARY" + cacheStatus;
    sb.append("│ ").append(String.format("%-76s", headerText)).append("│\n");
    sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
    sb.append("\n");
    sb.append("  Status:              [").append(success ? "SUCCESS" : "FAILED").append("]\n");
    sb.append("  API Calls:           ").append(apiCalls).append("\n");
    sb.append("  Errors:              ").append(errors).append("\n");
    
    // Execution Plan Statistics
    int planCount = 0;
    int planCacheHits = 0;
    int planCacheMisses = 0;
    int planErrors = 0;
    int planSuccesses = 0;
    for (ExecutionEvent event : events) {
      if ("execution_plan".equals(event.type)) {
        planCount++;
        if (event.data != null) {
          Object planCacheHitObj = event.data.get("planCacheHit");
          if (planCacheHitObj instanceof Boolean) {
            if ((Boolean) planCacheHitObj) {
              planCacheHits++;
            } else {
              planCacheMisses++;
            }
          }
          if (event.data.containsKey("error") && event.data.get("error") != null) {
            planErrors++;
          }
          if (event.data.containsKey("executionResult") && event.data.get("executionResult") != null) {
            planSuccesses++;
          }
        }
      }
    }
    if (planCount > 0) {
      sb.append("  Execution Plans:     ").append(planCount);
      if (planCacheHits > 0 || planCacheMisses > 0) {
        sb.append(" (").append(planCacheHits).append(" cache hit");
        if (planCacheHits != 1) sb.append("s");
        if (planCacheMisses > 0) {
          sb.append(", ").append(planCacheMisses).append(" miss");
          if (planCacheMisses != 1) sb.append("es");
        }
        sb.append(")");
      }
      sb.append("\n");
      if (planErrors > 0 || planSuccesses > 0) {
        sb.append("  Plan Results:        ");
        if (planSuccesses > 0) {
          sb.append(planSuccesses).append(" success");
          if (planSuccesses != 1) sb.append("es");
        }
        if (planErrors > 0) {
          if (planSuccesses > 0) sb.append(", ");
          sb.append(planErrors).append(" error");
          if (planErrors != 1) sb.append("s");
        }
        sb.append("\n");
      }
    }
    sb.append("\n");

    // LLM, Execution, and API Calls
    int llmCallNum = 1;
    int execCallNum = 1;
    int apiCallNum = 1;
    long totalPromptTokens = 0;
    long totalCompletionTokens = 0;
    long totalLLMDuration = 0;
    long totalExecutionDuration = 0;

    // Track phase counts for retry numbering
    Map<String, Integer> phaseCounts = new HashMap<>();

    // LLM Calls
    for (ExecutionEvent event : events) {
      if ("llm_inference_complete".equals(event.type)) {
        Object duration = event.data.get("durationMs");
        Object phase = event.data.get("phase");
        Object promptTokens = event.data.get("promptTokens");
        Object completionTokens = event.data.get("completionTokens");

        long promptT = 0;
        long completionT = 0;
        long dur = 0;
        if (promptTokens instanceof Number) {
          promptT = ((Number) promptTokens).longValue();
          totalPromptTokens += promptT;
        }
        if (completionTokens instanceof Number) {
          completionT = ((Number) completionTokens).longValue();
          totalCompletionTokens += completionT;
        }
        if (duration instanceof Number) {
          dur = ((Number) duration).longValue();
          totalLLMDuration += dur;
        }

        // Only process events with tokens > 0 (skip duplicate/fallback events)
        if (promptT == 0 && completionT == 0) {
          continue; // Skip this event
        }

        Object cacheHitObj = event.data.get("cacheHit");
        Boolean cacheHit = cacheHitObj instanceof Boolean ? (Boolean) cacheHitObj : null;
        
        String phaseStr =
            (phase != null && !phase.toString().equals("unknown")) ? phase.toString() : "?";
        // Track phase counts and append retry number if > 1
        int phaseCount = phaseCounts.getOrDefault(phaseStr, 0) + 1;
        phaseCounts.put(phaseStr, phaseCount);
        if (phaseCount > 1) {
          phaseStr = phaseStr + "#" + phaseCount;
        }
        // Cache status is shown in summary only, not in individual call headers
        // Format: "  LLM Call N (phase):    DURATIONms | Tokens: X+Y=Z"
        // Phase name without padding, then add padding before values
        String callLabel = String.format("  LLM Call %d (%s):", llmCallNum++, phaseStr);
        String durationStr = dur > 0 ? String.format("%6dms", dur) : "   N/A";
        // Compact token display: Tokens: 23432+233=23665
        String tokenStr =
            String.format("Tokens: %d+%d=%d", promptT, completionT, promptT + completionT);
        sb.append(String.format("%-30s %8s | %s", callLabel, durationStr, tokenStr)).append("\n");
      }
    }

    // Execution Calls (TypeScript execution)
    for (ExecutionEvent event : events) {
      if ("execution_phase".equals(event.type)) {
        Object duration = event.data.get("durationMs");
        Object attempt = event.data.get("attempt");
        Object execSuccessObj = event.data.get("success");
        Object error = event.data.get("error");
        
        long dur = 0;
        if (duration instanceof Number) {
          dur = ((Number) duration).longValue();
          totalExecutionDuration += dur;
        }
        
        int attemptNum = attempt instanceof Number ? ((Number) attempt).intValue() : execCallNum;
        boolean execSucceeded = execSuccessObj instanceof Boolean ? (Boolean) execSuccessObj : true;
        String errorStr = error != null ? error.toString() : null;
        
        String execLabel = String.format("  Execution %d:", execCallNum++);
        String durationStr = dur > 0 ? String.format("%6dms", dur) : "   N/A";
        String statusStr = execSucceeded ? "success" : (errorStr != null ? "error: " + errorStr : "failed");
        sb.append(String.format("%-20s %8s   %s", execLabel, durationStr, statusStr)).append("\n");
      }
    }

    // API Calls
    for (ExecutionEvent event : events) {
      if ("api_call".equals(event.type)) {
        Object duration = event.data.get("durationMs");
        Object url = event.data.get("url");
        if (duration != null) {
          String apiLabel = String.format("  API Call %d:", apiCallNum++);
          String durationStr =
              duration instanceof Number
                  ? String.format("%6dms", ((Number) duration).longValue())
                  : "   N/A";
          String urlStr = url != null ? url.toString() : "";
          sb.append(String.format("%-20s %8s   %s", apiLabel, durationStr, urlStr)).append("\n");
        }
      }
    }

    if (llmCallNum > 1) {
      sb.append("\n");
      sb.append("  Total LLM Duration:  ")
          .append(String.format("%6d", totalLLMDuration))
          .append("ms\n");
      if (totalPromptTokens > 0 || totalCompletionTokens > 0) {
        sb.append("  Total Tokens:        ")
            .append(totalPromptTokens)
            .append("+")
            .append(totalCompletionTokens)
            .append("=")
            .append(totalPromptTokens + totalCompletionTokens)
            .append("\n");
      } else {
        sb.append("  Total Tokens:        0\n");
      }
    } else if (llmCallNum == 1 && apiCallNum == 1) {
      sb.append("\n");
      sb.append("  No LLM or API calls recorded\n");
    }
    sb.append("\n");

    // Note: Normalized prompt schema section removed - will be re-enabled when caching mode is added

    // LLM Interactions - each call gets its own box header
    int llmInteractionNum = 1;
    String previousInputMessages = null;
    // Track phase counts for retry numbering
    Map<String, Integer> phaseCountsDetailed = new HashMap<>();
    for (int i = 0; i < events.size(); i++) {
      ExecutionEvent event = events.get(i);
      if ("llm_inference_complete".equals(event.type)) {
        Object duration = event.data.get("durationMs");
        Object phase = event.data.get("phase");
        Object response = event.data.get("response");

        Object promptTokens = event.data.get("promptTokens");
        Object completionTokens = event.data.get("completionTokens");

        long promptT = 0;
        long completionT = 0;
        if (promptTokens instanceof Number) promptT = ((Number) promptTokens).longValue();
        if (completionTokens instanceof Number)
          completionT = ((Number) completionTokens).longValue();

        // Skip events with 0 tokens (likely duplicate/fallback events)
        if (promptT == 0 && completionT == 0) {
          continue; // Skip this event
        }

        Object cacheHitObj = event.data.get("cacheHit");
        Boolean cacheHit = cacheHitObj instanceof Boolean ? (Boolean) cacheHitObj : null;
        
        String phaseStr =
            (phase != null && !phase.toString().equals("unknown")) ? phase.toString() : "?";
        // Track phase counts and append retry number if > 1
        int phaseCount = phaseCountsDetailed.getOrDefault(phaseStr, 0) + 1;
        phaseCountsDetailed.put(phaseStr, phaseCount);
        if (phaseCount > 1) {
          phaseStr = phaseStr + "#" + phaseCount;
        }
        // Cache status is shown in summary only, not in individual call headers
        String callHeader = "LLM Call " + llmInteractionNum + " (" + phaseStr + ")";

        // Box header for this LLM call
        sb.append(
            "┌──────────────────────────────────────────────────────────────────────────────┐\n");
        sb.append("│ ").append(String.format("%-76s", callHeader)).append(" │\n");
        sb.append(
            "└──────────────────────────────────────────────────────────────────────────────┘\n");
        sb.append("\n");

        llmInteractionNum++; // Increment after processing this event

        // Find corresponding input messages event (look backwards from current event)
        boolean foundInput = false;
        String currentInputMessages = null;
        for (int j = i - 1; j >= 0 && j >= i - 5; j--) { // Look back up to 5 events
          ExecutionEvent prevEvent = events.get(j);
          if ("llm_input_messages".equals(prevEvent.type)) {
            Object messages = prevEvent.data.get("messages");
            if (messages != null && !messages.toString().trim().isEmpty()) {
              currentInputMessages = messages.toString();
              foundInput = true;
              break;
            }
          }
        }

        if (foundInput && currentInputMessages != null) {
          // Check if input is the same as previous
          if (currentInputMessages.equals(previousInputMessages)) {
            sb.append(
                "┌─ INPUT ────────────────────────────────────────────────────────────────────────\n");
            sb.append("│ [Same as previous LLM call]\n");
            sb.append("\n");
          } else {
            sb.append(
                "┌─ INPUT ────────────────────────────────────────────────────────────────────────\n");
            String messagesStr = currentInputMessages;
            
            // If we have a previous input, elide common prefix
            if (previousInputMessages != null && !previousInputMessages.isEmpty()) {
              String elided = elideCommonPrefix(previousInputMessages, messagesStr);
              messagesStr = elided;
            }
            
            // Format messages nicely - they should already be formatted with [role] prefix
            String[] lines = messagesStr.split("\n");
            for (String line : lines) {
              if (line.isEmpty()) {
                sb.append("│\n");
              } else {
                // Wrap long lines
                int maxWidth = 76;
                if (line.length() <= maxWidth) {
                  sb.append("│ ").append(line).append("\n");
                } else {
                  // Split long lines
                  int start = 0;
                  while (start < line.length()) {
                    int end = Math.min(start + maxWidth, line.length());
                    String chunk = line.substring(start, end);
                    sb.append("│ ").append(chunk).append("\n");
                    start = end;
                  }
                }
              }
            }
            sb.append("\n");
            previousInputMessages = currentInputMessages;
          }
        } else {
          sb.append(
              "┌─ INPUT ────────────────────────────────────────────────────────────────────────\n");
          sb.append("│ [No input messages captured]\n");
          sb.append("\n");
        }

        if (response != null && !response.toString().trim().isEmpty()) {
          sb.append("┌─ OUTPUT (")
              .append(callHeader)
              .append(") ────────────────────────────────────────────────────────────────\n");
          String[] lines = response.toString().split("\n");
          for (String line : lines) {
            if (line.isEmpty()) {
              sb.append("│\n");
            } else {
              // Wrap long lines
              int maxWidth = 76;
              if (line.length() <= maxWidth) {
                sb.append("│ ").append(line).append("\n");
              } else {
                // Split long lines
                int start = 0;
                while (start < line.length()) {
                  int end = Math.min(start + maxWidth, line.length());
                  String chunk = line.substring(start, end);
                  sb.append("│ ").append(chunk).append("\n");
                  start = end;
                }
              }
            }
          }
          sb.append("\n");
        } else {
          sb.append("┌─ OUTPUT (")
              .append(callHeader)
              .append(") ────────────────────────────────────────────────────────────────\n");
          sb.append("│ [No response text captured]\n");
          sb.append("\n");
        }
      }
    }
    sb.append("\n");

    // Execution Plans (may have multiple due to retries)
    List<ExecutionEvent> planEvents = events.stream()
        .filter(e -> "execution_plan".equals(e.type))
        .collect(java.util.stream.Collectors.toList());
    
    if (!planEvents.isEmpty()) {
      int planNum = 1;
      for (ExecutionEvent event : planEvents) {
        sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
        sb.append("│ EXECUTION PLAN ").append(planEvents.size() > 1 ? String.format("(Attempt %d)", planNum) : "").append("\n");
        sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
        sb.append("\n");

        Object plan = event.data.get("plan");
        Object executionResult = event.data.get("executionResult");
        Object error = event.data.get("error");
        
        // Always show plan section, even if empty (so errors are visible)
        if (plan != null && !plan.toString().trim().isEmpty()) {
          String planStr = plan.toString();
          // Try to extract and pretty-print plan data from JSON
          try {
            Object parsed = JacksonUtility.getJsonMapper().readValue(planStr, Object.class);
            // Check if it's a JSON object with plan data
            if (parsed instanceof Map) {
              @SuppressWarnings("unchecked")
              Map<String, Object> planMap = (Map<String, Object>) parsed;
              
              // Show PS (Prompt Schema) information first
              sb.append("┌─ PROMPT SCHEMA (PS) ──────────────────────────────────────────────────\n");
              if (planMap.containsKey("ssql")) {
                sb.append("│ S-SQL: ").append(String.valueOf(planMap.get("ssql"))).append("\n");
              }
              if (planMap.containsKey("table")) {
                sb.append("│ Table: ").append(String.valueOf(planMap.get("table"))).append("\n");
              }
              if (planMap.containsKey("values")) {
                Object valuesObj = planMap.get("values");
                String valuesStr = JacksonUtility.getJsonMapper().writeValueAsString(valuesObj);
                sb.append("│ Values: ").append(valuesStr).append("\n");
              }
              if (planMap.containsKey("columns")) {
                Object columnsObj = planMap.get("columns");
                String columnsStr = JacksonUtility.getJsonMapper().writeValueAsString(columnsObj);
                sb.append("│ Columns: ").append(columnsStr).append("\n");
              }
              if (planMap.containsKey("cache_key")) {
                sb.append("│ Cache Key: ").append(String.valueOf(planMap.get("cache_key"))).append("\n");
              }
              sb.append("\n");
              
              // Show TypeScript code
              Object typescriptObj = planMap.get("typescript");
              if (typescriptObj != null) {
                sb.append("┌─ TYPESCRIPT PLAN ───────────────────────────────────────────────────\n");
                String typescriptCode = typescriptObj.toString();
                // Pretty-print the TypeScript code (basic formatting)
                typescriptCode = formatTypeScriptCode(typescriptCode);
                String[] lines = typescriptCode.split("\n");
                for (String line : lines) {
                  if (line.isEmpty()) {
                    sb.append("│\n");
                  } else {
                    int maxWidth = 76;
                    if (line.length() <= maxWidth) {
                      sb.append("│ ").append(line).append("\n");
                    } else {
                      int start = 0;
                      while (start < line.length()) {
                        int end = Math.min(start + maxWidth, line.length());
                        String chunk = line.substring(start, end);
                        sb.append("│ ").append(chunk).append("\n");
                        start = end;
                      }
                    }
                  }
                }
                sb.append("\n");
              } else {
                // No TypeScript, show full JSON
                sb.append("┌─ PLAN ─────────────────────────────────────────────────────────────────────\n");
                planStr = JacksonUtility.getJsonMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(parsed);
                String[] lines = planStr.split("\n");
                for (String line : lines) {
                  if (line.isEmpty()) {
                    sb.append("│\n");
                  } else {
                    int maxWidth = 76;
                    if (line.length() <= maxWidth) {
                      sb.append("│ ").append(line).append("\n");
                    } else {
                      int start = 0;
                      while (start < line.length()) {
                        int end = Math.min(start + maxWidth, line.length());
                        String chunk = line.substring(start, end);
                        sb.append("│ ").append(chunk).append("\n");
                        start = end;
                      }
                    }
                  }
                }
                sb.append("\n");
              }
            } else {
              // Not a map, use regular JSON pretty-printing
              planStr = JacksonUtility.getJsonMapper()
                  .writerWithDefaultPrettyPrinter()
                  .writeValueAsString(parsed);
              sb.append("┌─ PLAN ─────────────────────────────────────────────────────────────────────\n");
              String[] lines = planStr.split("\n");
              for (String line : lines) {
                if (line.isEmpty()) {
                  sb.append("│\n");
                } else {
                  int maxWidth = 76;
                  if (line.length() <= maxWidth) {
                    sb.append("│ ").append(line).append("\n");
                  } else {
                    int start = 0;
                    while (start < line.length()) {
                      int end = Math.min(start + maxWidth, line.length());
                      String chunk = line.substring(start, end);
                      sb.append("│ ").append(chunk).append("\n");
                      start = end;
                    }
                  }
                }
              }
              sb.append("\n");
            }
          } catch (Exception e) {
            // Not JSON, use as-is
            sb.append("┌─ PLAN ─────────────────────────────────────────────────────────────────────\n");
            String[] lines = planStr.split("\n");
            for (String line : lines) {
              if (line.isEmpty()) {
                sb.append("│\n");
              } else {
                int maxWidth = 76;
                if (line.length() <= maxWidth) {
                  sb.append("│ ").append(line).append("\n");
                } else {
                  int start = 0;
                  while (start < line.length()) {
                    int end = Math.min(start + maxWidth, line.length());
                    String chunk = line.substring(start, end);
                    sb.append("│ ").append(chunk).append("\n");
                    start = end;
                  }
                }
              }
            }
            sb.append("\n");
          }
        } else {
          // Show plan section even if empty, so execution result/error is visible
          sb.append("┌─ PLAN ─────────────────────────────────────────────────────────────────────\n");
          sb.append("│ [No execution plan captured]\n");
          sb.append("\n");
        }

        // Execution Result - show error or result for this attempt
        // Each attempt gets its own result section
        boolean hasError = error != null && !error.toString().trim().isEmpty();
        boolean hasResult = executionResult != null && !executionResult.toString().trim().isEmpty();
        
        if (hasError || hasResult) {
          sb.append("┌─ EXECUTION RESULT ─────────────────────────────────────────────────────────\n");
          
          if (hasError) {
            sb.append("│ [ERROR]\n");
            String errorStr = error.toString();
            
            // Strategy: Extract the most useful error message
            // 1. First try to extract from API call events (most reliable - direct from response body)
            // 2. Then try to extract from error message if it contains "API error:"
            // 3. Fall back to cleaned exception message
            
            String apiErrorMsg = extractApiErrorFromEvents(events, -1);
            
            if (apiErrorMsg == null || apiErrorMsg.trim().isEmpty()) {
              // Try to extract from error message itself if it contains "API error:"
              if (errorStr.contains("API error:")) {
                int apiErrorIndex = errorStr.indexOf("API error:");
                String afterApiError = errorStr.substring(apiErrorIndex + "API error:".length()).trim();
                // Extract the actual error message (may have HTTP status prefix)
                if (afterApiError.startsWith("HTTP ")) {
                  // Format: "API error: HTTP 400: Missing required fields..."
                  int colonIndex = afterApiError.indexOf(':', "HTTP ".length());
                  if (colonIndex > 0 && colonIndex < afterApiError.length() - 1) {
                    apiErrorMsg = afterApiError.substring(colonIndex + 1).trim();
                  } else {
                    // No second colon, use everything after "HTTP XXX"
                    apiErrorMsg = afterApiError;
                  }
                } else {
                  // Format: "API error: Missing required fields..."
                  apiErrorMsg = afterApiError;
                }
              }
            }
            
            if (apiErrorMsg != null && !apiErrorMsg.trim().isEmpty()) {
              // Use the API error message from response body instead of generic exception
              errorStr = apiErrorMsg;
              log.debug("Using extracted API error message: {}", apiErrorMsg);
            } else {
              log.debug("No API error message found, using exception message: {}", errorStr);
              // Clean up error string - remove stack trace information
              // If it looks like a compact stack trace (contains " > "), extract just the error message
              if (errorStr.contains(" > ")) {
                // Try to extract just the first meaningful part (exception type and message)
                // Look for patterns like "ClassName: message" or just "ClassName"
                String[] parts = errorStr.split(" > ");
                if (parts.length > 0) {
                  String firstPart = parts[0].trim();
                  // If first part contains a colon, try to extract message
                  int colonIndex = firstPart.indexOf(':');
                  if (colonIndex > 0 && colonIndex < firstPart.length() - 1) {
                    String className = firstPart.substring(0, colonIndex).trim();
                    String message = firstPart.substring(colonIndex + 1).trim();
                    // Only use if message doesn't look like a file path or stack trace
                    if (!message.contains("(") && !message.contains(".java")) {
                      errorStr = className + ": " + message;
                    } else {
                      errorStr = className;
                    }
                  } else {
                    // No colon, just use the first part (likely just class name)
                    errorStr = firstPart;
                  }
                }
              }
            }
            
            String[] errorLines = errorStr.split("\n");
            for (String line : errorLines) {
              if (line.isEmpty()) {
                sb.append("│\n");
              } else {
                int maxWidth = 76;
                if (line.length() <= maxWidth) {
                  sb.append("│ ").append(line).append("\n");
                } else {
                  int start = 0;
                  while (start < line.length()) {
                    int end = Math.min(start + maxWidth, line.length());
                    String chunk = line.substring(start, end);
                    sb.append("│ ").append(chunk).append("\n");
                    start = end;
                  }
                }
              }
            }
            sb.append("\n");
            
            // If this attempt failed but there's a result (from a later attempt), note it
            if (hasResult && planEvents.size() > 1) {
              sb.append("│ [Note: A later attempt succeeded]\n");
              sb.append("\n");
            }
          } else if (hasResult) {
            // Show result (only if no error for this attempt)
            String resultStr = executionResult.toString();
            // Try to parse and pretty-print as JSON
            try {
              Object parsed = JacksonUtility.getJsonMapper().readValue(resultStr, Object.class);
              resultStr =
                  JacksonUtility.getJsonMapper()
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(parsed);
            } catch (Exception e) {
              // Not JSON, use as-is
            }
            String[] resultLines = resultStr.split("\n");
            for (String line : resultLines) {
              if (line.isEmpty()) {
                sb.append("│\n");
              } else {
                int maxWidth = 76;
                if (line.length() <= maxWidth) {
                  sb.append("│ ").append(line).append("\n");
                } else {
                  int start = 0;
                  while (start < line.length()) {
                    int end = Math.min(start + maxWidth, line.length());
                    String chunk = line.substring(start, end);
                    sb.append("│ ").append(chunk).append("\n");
                    start = end;
                  }
                }
              }
            }
            sb.append("\n");
          }
        }
        
        planNum++;
        if (planNum <= planEvents.size()) {
          sb.append("\n"); // Space between multiple plans
        }
      }
    } else {
      sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
      sb.append("│ EXECUTION PLAN                                                               │\n");
      sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
      sb.append("\n");
      sb.append("  [No execution plan recorded]\n");
      sb.append("\n");
    }

    // API Calls - each call gets its own box header
    int apiCallNum2 = 1;
    boolean hasApiCalls = false;
    for (ExecutionEvent event : events) {
      if ("api_call".equals(event.type)) {
        hasApiCalls = true;
        Object method = event.data.get("method");
        Object url = event.data.get("url");
        Object statusCode = event.data.get("statusCode");
        Object duration = event.data.get("durationMs");
        Object requestBody = event.data.get("requestBody");
        Object responseBody = event.data.get("responseBody");

        // Build header with URL if available
        String apiHeader = "API Call " + apiCallNum2;
        if (url != null) {
          String urlStr = url.toString();
          // Truncate URL if too long to fit in header
          int maxUrlLength = 76 - apiHeader.length() - 3; // 3 for " - "
          if (urlStr.length() > maxUrlLength) {
            urlStr = urlStr.substring(0, maxUrlLength - 3) + "...";
          }
          apiHeader = apiHeader + " - " + urlStr;
        }

        // Box header for this API call
        sb.append(
            "┌──────────────────────────────────────────────────────────────────────────────┐\n");
        sb.append("│ ").append(String.format("%-76s", apiHeader)).append(" │\n");
        sb.append(
            "└──────────────────────────────────────────────────────────────────────────────┘\n");
        sb.append("\n");

        // Show method, status, and duration as regular content (URL already in header)
        StringBuilder details = new StringBuilder();
        if (method != null) {
          details.append("Method: ").append(method);
        }
        if (statusCode != null) {
          if (details.length() > 0) details.append(" | ");
          details.append("Status: ").append(statusCode);
        }
        if (duration != null) {
          if (details.length() > 0) details.append(" | ");
          details.append("Duration: ").append(duration).append("ms");
        }
        if (details.length() > 0) {
          sb.append("  ").append(details.toString()).append("\n");
          sb.append("\n");
        }

        apiCallNum2++;

        // Request Body
        if (requestBody != null && !requestBody.toString().trim().isEmpty()) {
          sb.append("┌─ REQUEST BODY ──────────────────────────────────────────────────────\n");
          // Try to pretty-print JSON
          String reqBodyStr = requestBody.toString();
          try {
            Object parsed = JacksonUtility.getJsonMapper().readValue(reqBodyStr, Object.class);
            reqBodyStr =
                JacksonUtility.getJsonMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(parsed);
          } catch (Exception e) {
            // Not JSON, use as-is
          }
          String[] lines = reqBodyStr.split("\n");
          for (String line : lines) {
            if (line.isEmpty()) {
              sb.append("│\n");
            } else {
              int maxWidth = 76;
              if (line.length() <= maxWidth) {
                sb.append("│ ").append(line).append("\n");
              } else {
                int start = 0;
                while (start < line.length()) {
                  int end = Math.min(start + maxWidth, line.length());
                  String chunk = line.substring(start, end);
                  sb.append("│ ").append(chunk).append("\n");
                  start = end;
                }
              }
            }
          }
          sb.append("\n");
        }

        // Response Body
        if (responseBody != null && !responseBody.toString().trim().isEmpty()) {
          sb.append("┌─ RESPONSE BODY ─────────────────────────────────────────────────────\n");
          // Try to pretty-print JSON
          String respBodyStr = responseBody.toString();
          try {
            Object parsed = JacksonUtility.getJsonMapper().readValue(respBodyStr, Object.class);
            respBodyStr =
                JacksonUtility.getJsonMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(parsed);
          } catch (Exception e) {
            // Not JSON, use as-is
          }
          String[] lines = respBodyStr.split("\n");
          for (String line : lines) {
            if (line.isEmpty()) {
              sb.append("│\n");
            } else {
              int maxWidth = 76;
              if (line.length() <= maxWidth) {
                sb.append("│ ").append(line).append("\n");
              } else {
                int start = 0;
                while (start < line.length()) {
                  int end = Math.min(start + maxWidth, line.length());
                  String chunk = line.substring(start, end);
                  sb.append("│ ").append(chunk).append("\n");
                  start = end;
                }
              }
            }
          }
          sb.append("\n");
        } else {
          sb.append("┌─ RESPONSE BODY ─────────────────────────────────────────────────────\n");
          sb.append("│ [No response body]\n");
          sb.append("\n");
        }

        // cURL Command - no left edge so users can copy-paste directly
        sb.append("┌─ cURL COMMAND ──────────────────────────────────────────────────────────\n");
        StringBuilder curlCmd =
            new StringBuilder("curl -X ").append(method != null ? method : "GET");
        if (url != null) {
          curlCmd.append(" '").append(url).append("'");
        }
        curlCmd.append(" \\\n");
        curlCmd.append("      -H 'Accept: application/json'");
        if (requestBody != null && !requestBody.toString().trim().isEmpty()) {
          curlCmd.append(" \\\n");
          curlCmd.append("      -H 'Content-Type: application/json'");
          curlCmd.append(" \\\n");
          // Escape single quotes in the body for shell safety
          String escapedBody = requestBody.toString().replace("'", "'\\''");
          // Always include the full body - do not truncate
          curlCmd.append("      -d '").append(escapedBody).append("'\n");
        } else {
          curlCmd.append("\n");
        }
        sb.append(curlCmd.toString());
        sb.append("\n");
        sb.append("\n");
      }
    }
    if (!hasApiCalls) {
      sb.append("  [No API calls recorded]\n");
    }
    sb.append("\n");

    // Final Response
    sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    sb.append("│ FINAL RESPONSE                                                               │\n");
    sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
    sb.append("\n");

    for (ExecutionEvent event : events) {
      if ("final_response".equals(event.type)) {
        Object response = event.data.get("response");
        if (response != null) {
          String responseStr = response.toString();
          // Try to parse and pretty-print as JSON
          try {
            Object parsed = JacksonUtility.getJsonMapper().readValue(responseStr, Object.class);
            responseStr =
                JacksonUtility.getJsonMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(parsed);
          } catch (Exception e) {
            // Not JSON, use as-is
          }
          String[] lines = responseStr.split("\n");
          for (String line : lines) {
            sb.append("  ").append(line).append("\n");
          }
        }
      }
    }
    sb.append("\n");

    // Assignment Result
    sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    sb.append("│ ASSIGNMENT RESULT                                                            │\n");
    sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
    sb.append("\n");

    boolean hasAssignmentResult = false;
    for (ExecutionEvent event : events) {
      if ("assignment_result".equals(event.type)) {
        hasAssignmentResult = true;
        Object assignmentResultJson = event.data.get("assignmentResult");
        if (assignmentResultJson != null && !assignmentResultJson.toString().trim().isEmpty()) {
          String resultStr = assignmentResultJson.toString();
          // Try to parse and pretty-print as JSON
          try {
            Object parsed = JacksonUtility.getJsonMapper().readValue(resultStr, Object.class);
            resultStr =
                JacksonUtility.getJsonMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(parsed);
          } catch (Exception e) {
            // Not JSON, use as-is
          }
          String[] lines = resultStr.split("\n");
          for (String line : lines) {
            sb.append("  ").append(line).append("\n");
          }
        } else {
          sb.append("  [No assignment result data captured]\n");
        }
        sb.append("\n");
        break; // Only show first assignment result
      }
    }
    if (!hasAssignmentResult) {
      sb.append("  [No assignment result recorded]\n");
      sb.append("\n");
    }

    // Server Log Section
    sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    sb.append("│ SERVER LOG                                                                   │\n");
    sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
    sb.append("\n");
    
    String serverLog = captureServerLog(startTimeMs, durationMs);
    if (serverLog != null && !serverLog.trim().isEmpty()) {
      String[] lines = serverLog.split("\n");
      for (String line : lines) {
        if (line.isEmpty()) {
          sb.append("│\n");
        } else {
          int maxWidth = 76;
          if (line.length() <= maxWidth) {
            sb.append("│ ").append(line).append("\n");
          } else {
            int start = 0;
            while (start < line.length()) {
              int end = Math.min(start + maxWidth, line.length());
              String chunk = line.substring(start, end);
              sb.append("│ ").append(chunk).append("\n");
              start = end;
            }
          }
        }
      }
      sb.append("\n");
    } else {
      sb.append("│ [No server log entries captured during execution]\n");
      sb.append("\n");
    }

    // Footer
    sb.append("╔══════════════════════════════════════════════════════════════════════════════╗\n");
    sb.append("║                          END OF REPORT                                       ║\n");
    sb.append("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    return sb.toString();
  }

  // Event logging methods

  public void logLlmInferenceStart(String phase) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        events.add(
            new ExecutionEvent(
                "llm_inference_start",
                executionId,
                Instant.now().toString(),
                Map.of("phase", phase != null ? phase : "unknown")));
      }
    }

    log.debug("LLM inference started (phase: {})", phase);
  }

  public void logLlmInferenceComplete(
      String phase, long durationMs, long promptTokens, long completionTokens, String response) {
    logLlmInferenceComplete(phase, durationMs, promptTokens, completionTokens, response, null);
  }

  public void logLlmInferenceComplete(
      String phase, long durationMs, long promptTokens, long completionTokens, String response, Boolean cacheHit) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("phase", phase != null ? phase : "unknown");
        data.put("durationMs", durationMs);
        data.put("promptTokens", promptTokens);
        data.put("completionTokens", completionTokens);
        data.put("response", response != null ? response : "");
        if (cacheHit != null) {
          data.put("cacheHit", cacheHit);
        }
        events.add(
            new ExecutionEvent(
                "llm_inference_complete", executionId, Instant.now().toString(), data));
      }
    }

    log.info(
        "LLM inference completed (phase: {}, duration: {}ms, tokens: {})",
        phase,
        durationMs,
        promptTokens + completionTokens);
  }

  public void logLlmInputMessages(List<LlmClient.Message> messages) {
    String executionId = currentExecutionId.get();
    if (executionId == null) {
      log.warn("Cannot log LLM input messages: execution ID is null");
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        // Format messages nicely
        StringBuilder msgBuilder = new StringBuilder();
        if (messages != null) {
          for (LlmClient.Message msg : messages) {
            msgBuilder
                .append("[")
                .append(msg.role())
                .append("] ")
                .append(msg.content())
                .append("\n");
          }
        }
        events.add(
            new ExecutionEvent(
                "llm_input_messages",
                executionId,
                Instant.now().toString(),
                Map.of("messages", msgBuilder.toString().trim())));
        log.debug(
            "LLM input messages logged (count: {}, executionId: {})",
            messages != null ? messages.size() : 0,
            executionId);
      } else {
        log.warn(
            "Cannot log LLM input messages: events list is null for executionId: {}", executionId);
      }
    } else {
      log.debug("Report mode disabled, skipping LLM input messages logging");
    }
  }

  public void logToolCall(String toolName, Map<String, Object> arguments) {
    String executionId = currentExecutionId.get();
    if (executionId == null) {
      log.warn("Cannot log tool call: execution ID is null (tool: {})", toolName);
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        // Format arguments as JSON string for better readability
        String argsJson = "{}";
        if (arguments != null && !arguments.isEmpty()) {
          try {
            argsJson = JacksonUtility.getJsonMapper().writeValueAsString(arguments);
          } catch (Exception e) {
            argsJson = arguments.toString();
          }
        }
        events.add(
            new ExecutionEvent(
                "tool_call",
                executionId,
                Instant.now().toString(),
                Map.of(
                    "toolName", toolName != null ? toolName : "unknown", "arguments", argsJson)));
        log.debug("Tool call logged: {} (executionId: {})", toolName, executionId);
      } else {
        log.warn(
            "Cannot log tool call: events list is null for executionId: {} (tool: {})",
            executionId,
            toolName);
      }
    } else {
      log.debug("Report mode disabled, skipping tool call logging");
    }
    log.info("Tool call: {} (args: {})", toolName, arguments != null ? arguments.size() : 0);
  }

  public void logToolOutput(String toolName, Object output) {
    String executionId = currentExecutionId.get();
    if (executionId == null) {
      log.warn("Cannot log tool output: execution ID is null (tool: {})", toolName);
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        // Find the last tool_call event and add output to it
        boolean found = false;
        for (int i = events.size() - 1; i >= 0; i--) {
          ExecutionEvent event = events.get(i);
          if ("tool_call".equals(event.type) && toolName.equals(event.data.get("toolName"))) {
            event.data.put("output", output != null ? output.toString() : "");
            found = true;
            log.debug(
                "Tool output added to tool_call event: {} (executionId: {})",
                toolName,
                executionId);
            break;
          }
        }
        if (!found) {
          log.warn(
              "Could not find tool_call event for tool: {} (executionId: {})",
              toolName,
              executionId);
        }
      } else {
        log.warn(
            "Cannot log tool output: events list is null for executionId: {} (tool: {})",
            executionId,
            toolName);
      }
    } else {
      log.debug("Report mode disabled, skipping tool output logging");
    }
    log.debug("Tool output logged: {}", toolName);
  }

  public void logApiCall(
      String method,
      String url,
      int statusCode,
      long durationMs,
      String requestBody,
      String responseBody) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("method", method != null ? method : "UNKNOWN");
        data.put("url", url != null ? url : "");
        data.put("statusCode", statusCode);
        data.put("durationMs", durationMs);
        data.put("requestBody", requestBody != null ? requestBody : "");
        data.put("responseBody", responseBody != null ? responseBody : "");
        events.add(new ExecutionEvent("api_call", executionId, Instant.now().toString(), data));
      }
    }

    log.info("API call: {} {} (status: {}, duration: {}ms)", method, url, statusCode, durationMs);
  }

  public void logApiCallError(String method, String url, String error) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("method", method != null ? method : "UNKNOWN");
        data.put("url", url != null ? url : "");
        data.put("error", error != null ? error : "");
        events.add(
            new ExecutionEvent("api_call_error", executionId, Instant.now().toString(), data));
      }
    }

    log.warn("API call error: {} {} - {}", method, url, error);
  }

  public void logCodeGeneration(String code, boolean success, String error) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("code", code != null ? code : "");
        data.put("success", success);
        if (error != null) {
          data.put("error", error);
        }
        events.add(
            new ExecutionEvent("code_generation", executionId, Instant.now().toString(), data));
      }
    }

    log.info("Code generation: {} (success: {})", code != null ? code.length() : 0, success);
  }

  public void logExecutionPlan(String planJson) {
    logExecutionPlan(planJson, null, null, null);
  }

  public void logExecutionPlan(String planJson, Boolean planCacheHit) {
    logExecutionPlan(planJson, null, null, planCacheHit);
  }

  public void logExecutionPlan(String planJson, String executionResult, String error) {
    logExecutionPlan(planJson, executionResult, error, null);
  }

  /**
   * Log execution plan as a new event, or update the most recent plan event if it doesn't have a result/error yet.
   * Use this when logging each attempt separately.
   */
  public void logExecutionPlanNew(String planJson, String executionResult, String error, Boolean planCacheHit) {
    String executionId = currentExecutionId.get();
    if (executionId == null) {
      log.warn("Cannot log execution plan: executionId is null");
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        // If we're adding a result/error and have a plan JSON, try to update the most recent plan event
        // that doesn't have a result/error yet (this handles the case where plan was logged first, then result/error)
        if ((executionResult != null || error != null) && planJson != null && !planJson.trim().isEmpty()) {
          // Find the most recent execution_plan event that doesn't have a result or error
          for (int i = events.size() - 1; i >= 0; i--) {
            ExecutionEvent event = events.get(i);
            if ("execution_plan".equals(event.type) && event.data != null) {
              boolean hasResult = event.data.containsKey("executionResult") && event.data.get("executionResult") != null;
              boolean hasError = event.data.containsKey("error") && event.data.get("error") != null;
              if (!hasResult && !hasError) {
                // Found a plan event without result/error - update it
                event.data.put("plan", planJson);
                if (executionResult != null) {
                  event.data.put("executionResult", executionResult);
                }
                if (error != null) {
                  event.data.put("error", error);
                }
                if (planCacheHit != null) {
                  event.data.put("planCacheHit", planCacheHit);
                }
                log.debug("Updated existing execution plan event with result/error (executionId: {}, hasResult: {}, hasError: {}, planCacheHit: {})", 
                    executionId, executionResult != null, error != null, planCacheHit);
                return;
              }
            }
          }
        }
        
        // No existing event to update, or we're logging a new plan - create a new event
        Map<String, Object> data = new HashMap<>();
        data.put("plan", planJson != null ? planJson : "");
        if (executionResult != null) {
          data.put("executionResult", executionResult);
        }
        if (error != null) {
          data.put("error", error);
        }
        if (planCacheHit != null) {
          data.put("planCacheHit", planCacheHit);
        }
        events.add(
            new ExecutionEvent("execution_plan", executionId, Instant.now().toString(), data));
        log.debug("Execution plan logged as new event (length: {}, executionId: {}, hasResult: {}, hasError: {}, planCacheHit: {})", 
            planJson != null ? planJson.length() : 0, executionId, executionResult != null, error != null, planCacheHit);
      } else {
        log.warn("Cannot log execution plan: events list is null for executionId: {}", executionId);
      }
    } else {
      log.debug("Execution plan not logged: report mode disabled");
    }
  }

  public void logExecutionPlan(String planJson, String executionResult, String error, Boolean planCacheHit) {
    String executionId = currentExecutionId.get();
    if (executionId == null) {
      log.warn("Cannot log execution plan: executionId is null");
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        // Try to update the most recent plan event if we're just adding cache status or results
        // This handles cases where plan was logged early and we're now adding cache status
        if ((executionResult != null || error != null) && (planJson == null || planJson.trim().isEmpty())) {
          // We're only adding execution results to an existing plan (no plan JSON provided)
          // Find the most recent execution_plan event and update it
          for (int i = events.size() - 1; i >= 0; i--) {
            ExecutionEvent event = events.get(i);
            if ("execution_plan".equals(event.type) && event.data != null) {
              // Update the existing event's data map (map is mutable even though reference is final)
              if (executionResult != null) {
                event.data.put("executionResult", executionResult);
              }
              if (error != null) {
                event.data.put("error", error);
              }
              if (planCacheHit != null) {
                event.data.put("planCacheHit", planCacheHit);
              }
              log.debug("Updated execution plan with result (executionId: {}, hasResult: {}, hasError: {}, planCacheHit: {})", 
                  executionId, executionResult != null, error != null, planCacheHit);
              return;
            }
          }
          log.warn("No existing plan event found to update with result (executionId: {})", executionId);
        } else if (planCacheHit != null && planJson != null && !planJson.trim().isEmpty()) {
          // We have both plan and cache status - try to update existing event first
          // (This handles the case where plan was logged early without cache status)
          for (int i = events.size() - 1; i >= 0; i--) {
            ExecutionEvent event = events.get(i);
            if ("execution_plan".equals(event.type) && event.data != null) {
              // Update the existing event's data map
              event.data.put("plan", planJson);
              if (executionResult != null) {
                event.data.put("executionResult", executionResult);
              }
              if (error != null) {
                event.data.put("error", error);
              }
              event.data.put("planCacheHit", planCacheHit);
              log.debug("Updated execution plan with cache status (executionId: {}, planCacheHit: {})", 
                  executionId, planCacheHit);
              return;
            }
          }
          // No existing event found - will create new one below
        }
        
        // Log new plan event - either we have a plan JSON, or we couldn't find an existing event to update
        Map<String, Object> data = new HashMap<>();
        data.put("plan", planJson != null ? planJson : "");
        if (executionResult != null) {
          data.put("executionResult", executionResult);
        }
        if (error != null) {
          data.put("error", error);
        }
        if (planCacheHit != null) {
          data.put("planCacheHit", planCacheHit);
          log.debug("Setting planCacheHit to {} in execution plan event", planCacheHit);
        } else {
          log.debug("planCacheHit is null - not setting cache status in execution plan event");
        }
        events.add(
            new ExecutionEvent("execution_plan", executionId, Instant.now().toString(), data));
        log.debug("Execution plan logged (length: {}, executionId: {}, hasResult: {}, hasError: {}, planCacheHit: {})", 
            planJson != null ? planJson.length() : 0, executionId, executionResult != null, error != null, planCacheHit);
      } else {
        log.warn("Cannot log execution plan: events list is null for executionId: {}", executionId);
      }
    } else {
      log.debug("Execution plan not logged: report mode disabled");
    }
  }

  public void logPhaseChange(String phase) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        events.add(
            new ExecutionEvent(
                "phase_change",
                executionId,
                Instant.now().toString(),
                Map.of("phase", phase != null ? phase : "unknown")));
      }
    }

    log.debug("Phase change: {}", phase);
  }

  /**
   * Log execution phase (TypeScript execution) with duration and result.
   */
  public void logExecutionPhase(int attempt, long durationMs, boolean success, String error) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("attempt", attempt);
        data.put("durationMs", durationMs);
        data.put("success", success);
        if (error != null) {
          data.put("error", error);
        }
        events.add(
            new ExecutionEvent(
                "execution_phase", executionId, Instant.now().toString(), data));
      }
    }

    log.info(
        "Execution phase completed (attempt: {}, duration: {}ms, success: {})",
        attempt,
        durationMs,
        success);
  }

  public void logStepExecutionResult(String stepId, Object result) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        events.add(
            new ExecutionEvent(
                "step_execution_result",
                executionId,
                Instant.now().toString(),
                Map.of(
                    "stepId",
                    stepId != null ? stepId : "",
                    "result",
                    result != null ? result.toString() : "")));
      }
    }

    log.debug("Step execution result: {}", stepId);
  }

  public void logFinalResponse(String response) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        events.add(
            new ExecutionEvent(
                "final_response",
                executionId,
                Instant.now().toString(),
                Map.of("response", response != null ? response : "")));
      }
    }

    log.debug("Final response logged");
  }

  public void logNormalizedPromptSchema(String normalizedSchemaJson, long durationMs) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;
    logNormalizedPromptSchemaForExecution(normalizedSchemaJson, durationMs, executionId);
  }

  public void logNormalizedPromptSchemaForExecution(
      String normalizedSchemaJson, long durationMs, String executionId) {
    if (executionId == null) {
      log.warn("Cannot log normalized prompt schema: execution ID is null");
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("schema", normalizedSchemaJson != null ? normalizedSchemaJson : "");
        data.put("durationMs", durationMs);
        data.put("background", true);
        events.add(
            new ExecutionEvent(
                "normalized_prompt_schema", executionId, Instant.now().toString(), data));
        log.debug(
            "Normalized prompt schema logged (background, {}ms, executionId: {})",
            durationMs,
            executionId);
      } else {
        log.warn(
            "Cannot log normalized prompt schema: events list is null for executionId: {}",
            executionId);
      }
    } else {
      log.debug("Report mode disabled, skipping normalized prompt schema logging");
    }
  }

  public void logAssigmentResult(AssigmentResult assignmentResult) {
    String executionId = currentExecutionId.get();
    if (executionId == null) {
      log.warn("Cannot log AssigmentResult: execution ID is null");
      return;
    }

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        try {
          // Serialize AssigmentResult to JSON
          String assignmentResultJson =
              JacksonUtility.getJsonMapper().writeValueAsString(assignmentResult);
          Map<String, Object> data = new HashMap<>();
          data.put("assignmentResult", assignmentResultJson);
          events.add(
              new ExecutionEvent("assignment_result", executionId, Instant.now().toString(), data));
          log.debug("AssigmentResult logged (executionId: {})", executionId);
        } catch (Exception e) {
          log.warn(
              "Failed to serialize AssigmentResult to JSON (executionId: {}): {}",
              executionId,
              e.getMessage());
        }
      } else {
        log.warn(
            "Cannot log AssigmentResult: events list is null for executionId: {}", executionId);
      }
    } else {
      log.debug("Report mode disabled, skipping AssigmentResult logging");
    }
  }

  /**
   * Elide common prefix between two strings (typically LLM input messages).
   * Compares line by line and removes matching prefix lines, replacing them with
   * an elision marker.
   *
   * @param previous the previous string
   * @param current the current string
   * @return the current string with common prefix elided
   */
  private String elideCommonPrefix(String previous, String current) {
    if (previous == null || previous.isEmpty() || current == null || current.isEmpty()) {
      return current;
    }

    String[] prevLines = previous.split("\n");
    String[] currLines = current.split("\n");

    // Find the number of matching lines from the start
    int commonLines = 0;
    int minLines = Math.min(prevLines.length, currLines.length);
    for (int i = 0; i < minLines; i++) {
      if (prevLines[i].equals(currLines[i])) {
        commonLines++;
      } else {
        break;
      }
    }

    // If no common prefix or all lines are common, return current as-is
    if (commonLines == 0 || commonLines >= currLines.length) {
      return current;
    }

    // If there's a significant common prefix (at least 2 lines), elide it
    if (commonLines >= 2) {
      StringBuilder result = new StringBuilder();
      result.append("... (")
          .append(commonLines)
          .append(" line")
          .append(commonLines > 1 ? "s" : "")
          .append(" from previous call elided) ...\n");
      
      // Append the unique part
      for (int i = commonLines; i < currLines.length; i++) {
        result.append(currLines[i]);
        if (i < currLines.length - 1) {
          result.append("\n");
        }
      }
      return result.toString();
    }

    // If only 1 line matches, don't elide (not significant enough)
    return current;
  }

  /**
   * Format TypeScript code for pretty printing in reports.
   */
  private String formatTypeScriptCode(String code) {
    if (code == null || code.trim().isEmpty()) {
      return code;
    }
    
    // Unescape JSON string if needed
    if (code.startsWith("\"") && code.endsWith("\"")) {
      try {
        code = JacksonUtility.getJsonMapper().readValue(code, String.class);
      } catch (Exception e) {
        // Not a JSON string, use as-is
      }
    }
    
    // Replace escaped newlines with actual newlines
    code = code.replace("\\n", "\n");
    code = code.replace("\\t", "  ");
    
    // Basic formatting: ensure consistent indentation
    StringBuilder formatted = new StringBuilder();
    int indent = 0;
    String[] lines = code.split("\n");
    
    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        formatted.append("\n");
        continue;
      }
      
      // Decrease indent before closing braces
      if (trimmed.startsWith("}") || trimmed.startsWith("]")) {
        indent = Math.max(0, indent - 2);
      }
      
      // Add indentation
      for (int i = 0; i < indent; i++) {
        formatted.append(" ");
      }
      formatted.append(trimmed).append("\n");
      
      // Increase indent after opening braces
      if (trimmed.endsWith("{") || trimmed.endsWith("[")) {
        indent += 2;
      }
    }
    
    return formatted.toString().trim();
  }

  /**
   * Extract API error message from the most recent API call event with an error status.
   * Looks for API calls with status >= 400 and extracts the error message from the response body.
   * 
   * Since API calls happen during execution (after the plan event is created), we look through
   * ALL events to find the most recent API call with an error status.
   *
   * @param events list of execution events
   * @param currentEventIndex index of the current event (execution_plan with error) - not used, kept for API compatibility
   * @return the API error message, or null if not found
   */
  private String extractApiErrorFromEvents(List<ExecutionEvent> events, int currentEventIndex) {
    if (events == null || events.isEmpty()) {
      log.debug("extractApiErrorFromEvents: events is null or empty");
      return null;
    }
    
    // Look through ALL events to find the most recent API call with error status
    // API calls happen during execution, so they may be anywhere in the events list
    ExecutionEvent mostRecentErrorApiCall = null;
    int mostRecentIndex = -1;
    
    for (int i = 0; i < events.size(); i++) {
      ExecutionEvent event = events.get(i);
      if ("api_call".equals(event.type) && event.data != null) {
        Object statusCode = event.data.get("statusCode");
        
        // Check if this is an error status (4xx or 5xx)
        if (statusCode != null) {
          int status = 0;
          if (statusCode instanceof Number) {
            status = ((Number) statusCode).intValue();
          } else {
            try {
              status = Integer.parseInt(statusCode.toString());
            } catch (NumberFormatException e) {
              continue;
            }
          }
          
          if (status >= 400) {
            // This is an error API call - track it as the most recent
            mostRecentErrorApiCall = event;
            mostRecentIndex = i;
            log.debug("Found error API call at index {} with status {}", i, status);
          }
        }
      }
    }
    
    if (mostRecentErrorApiCall == null) {
      log.debug("extractApiErrorFromEvents: No error API call found in {} events", events.size());
      return null;
    }
    
    log.debug("extractApiErrorFromEvents: Found error API call at index {}, extracting error message", mostRecentIndex);
    
    // If we found an error API call, extract the error message from its response body
    if (mostRecentErrorApiCall.data != null) {
      Object responseBody = mostRecentErrorApiCall.data.get("responseBody");
      if (responseBody != null) {
        String responseBodyStr = responseBody.toString();
        log.debug("extractApiErrorFromEvents: Response body length: {}", responseBodyStr.length());
        if (!responseBodyStr.trim().isEmpty()) {
          // Try to extract error message from JSON response
          try {
            JsonNode responseJson = JacksonUtility.getJsonMapper().readTree(responseBodyStr);
            if (responseJson.has("error") && responseJson.get("error").isTextual()) {
              String errorMsg = responseJson.get("error").asText();
              log.debug("extractApiErrorFromEvents: Extracted error message: {}", errorMsg);
              return errorMsg;
            } else if (responseJson.has("message") && responseJson.get("message").isTextual()) {
              String errorMsg = responseJson.get("message").asText();
              log.debug("extractApiErrorFromEvents: Extracted message: {}", errorMsg);
              return errorMsg;
            } else if (responseJson.has("error") && !responseJson.get("error").isNull()) {
              String errorMsg = responseJson.get("error").toString();
              log.debug("extractApiErrorFromEvents: Extracted error (non-textual): {}", errorMsg);
              return errorMsg;
            }
            log.debug("extractApiErrorFromEvents: JSON response found but no error/message field");
          } catch (Exception e) {
            log.debug("extractApiErrorFromEvents: Failed to parse response as JSON: {}", e.getMessage());
            // Not JSON or parse error, try to extract from response body as-is
            // Look for common error patterns
            if (responseBodyStr.contains("\"error\"") || responseBodyStr.contains("error:")) {
              // Try to extract error message from text
              int errorIndex = responseBodyStr.indexOf("\"error\"");
              if (errorIndex >= 0) {
                int colonIndex = responseBodyStr.indexOf(':', errorIndex);
                if (colonIndex > 0 && colonIndex < responseBodyStr.length() - 1) {
                  String afterColon = responseBodyStr.substring(colonIndex + 1).trim();
                  // Extract quoted string or value
                  if (afterColon.startsWith("\"")) {
                    int endQuote = afterColon.indexOf('"', 1);
                    if (endQuote > 0) {
                      String errorMsg = afterColon.substring(1, endQuote);
                      log.debug("extractApiErrorFromEvents: Extracted error from text pattern: {}", errorMsg);
                      return errorMsg;
                    }
                  }
                }
              }
            }
          }
        } else {
          log.debug("extractApiErrorFromEvents: Response body is empty");
        }
      } else {
        log.debug("extractApiErrorFromEvents: Response body is null");
      }
    } else {
      log.debug("extractApiErrorFromEvents: Event data is null");
    }
    
    log.debug("extractApiErrorFromEvents: Could not extract error message");
    return null;
  }

  /**
   * Capture server log entries that occurred during the execution timeframe.
   *
   * @param startTimeMs execution start time in milliseconds
   * @param durationMs execution duration in milliseconds
   * @return log entries as a string, or null if unable to capture
   */
  private String captureServerLog(long startTimeMs, long durationMs) {
    if (startTimeMs == 0) {
      return null; // No start time provided, can't capture logs
    }

    try {
      // Determine log file location (same logic as OneMcp.configureFileOnlyLogging)
      String logDirEnv = System.getenv("ONEMCP_LOG_DIR");
      java.io.File logFile;
      if (logDirEnv != null && !logDirEnv.isBlank()) {
        logFile = new java.io.File(logDirEnv, "onemcp.log");
      } else {
        String homeDirEnv = System.getenv("ONEMCP_HOME_DIR");
        if (homeDirEnv != null && !homeDirEnv.isBlank()) {
          logFile = new java.io.File(homeDirEnv, "logs/onemcp.log");
        } else {
          String userHome = System.getProperty("user.home");
          logFile = new java.io.File(
              userHome != null ? userHome : System.getProperty("java.io.tmpdir"),
              ".onemcp/logs/onemcp.log");
        }
      }

      if (!logFile.exists() || !logFile.canRead()) {
        log.debug("Log file not found or not readable: {}", logFile);
        return null;
      }

      // Read log file and filter entries within execution timeframe
      long endTimeMs = startTimeMs + durationMs;
      List<String> relevantLines = new ArrayList<>();
      
      try (java.io.BufferedReader reader = java.nio.file.Files.newBufferedReader(logFile.toPath())) {
        String line;
        while ((line = reader.readLine()) != null) {
          // Try to extract timestamp from log line (format varies by logback config)
          // Common formats: "2025-12-01 05:27:32.123" or ISO format
          // For now, include all lines - timestamp parsing can be improved later
          relevantLines.add(line);
        }
      }

      // If log file is too large, only return recent lines (last 1000 lines)
      if (relevantLines.size() > 1000) {
        relevantLines = relevantLines.subList(relevantLines.size() - 1000, relevantLines.size());
      }

      return String.join("\n", relevantLines);
    } catch (Exception e) {
      log.debug("Failed to capture server log: {}", e.getMessage());
      return null;
    }
  }

  /** Internal data structure for execution events. */
  public static class ExecutionEvent {
    public final String type;
    public final String executionId;
    public final String timestamp;
    public final Map<String, Object> data;

    public ExecutionEvent(
        String type, String executionId, String timestamp, Map<String, Object> data) {
      this.type = type;
      this.executionId = executionId;
      this.timestamp = timestamp;
      this.data = data != null ? new HashMap<>(data) : new HashMap<>();
    }
  }
}
