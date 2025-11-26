package com.gentoro.onemcp.logging;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.context.KnowledgeBase;
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
import java.util.UUID;
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

  // Track report paths per execution
  private final Map<String, Path> executionReportPaths = new ConcurrentHashMap<>();

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
      KnowledgeBase knowledgeBase = oneMcp.knowledgeBase();
      if (knowledgeBase != null) {
        Path handbookPath = knowledgeBase.handbookPath();
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
   * <ol>
   *   <li>Environment variable {@code ONEMCP_LOG_DIR} (CLI mode: set to {@code {handbook}/logs})
   *   <li>Config file {@code logging.directory}
   *   <li>If handbook mode detected: use {@code {handbook}/logs/} (fallback for CLI mode)
   *   <li>Default: {@code /var/log/onemcp} (production mode)
   * </ol>
   *
   * <p>Behavior:
   * <ul>
   *   <li><b>CLI mode</b>: CLI sets {@code ONEMCP_LOG_DIR} to {@code {handbook}/logs}, so reports go to
   *       {@code {handbook}/logs/reports/}
   *   <li><b>Production mode</b>: When {@code ONEMCP_LOG_DIR} is not set and no handbook is detected,
   *       defaults to {@code /var/log/onemcp/reports/}
   * </ul>
   *
   * <p>Reports are always stored in {@code {logging_dir}/reports/} subdirectory.
   */
  private Path determineReportsDirectory() {
    Path baseLogDir;

    // Priority 1: Environment variable (CLI mode sets ONEMCP_LOG_DIR to {handbook}/logs)
    String envLogDir = System.getenv("ONEMCP_LOG_DIR");
    if (envLogDir != null && !envLogDir.isBlank()) {
      baseLogDir = Paths.get(envLogDir);
      log.debug("Using logging directory from ONEMCP_LOG_DIR: {}", baseLogDir);
    } else {
      // Priority 2: Config file
      Configuration config = oneMcp.configuration();
      String configLogDir = config != null ? config.getString("logging.directory", null) : null;
      if (configLogDir != null && !configLogDir.isBlank()) {
        baseLogDir = Paths.get(configLogDir);
        log.debug("Using logging directory from config: {}", baseLogDir);
      } else {
        // Priority 3: Try handbook mode (fallback for CLI mode when ONEMCP_LOG_DIR not set)
        try {
          KnowledgeBase knowledgeBase = oneMcp.knowledgeBase();
          if (knowledgeBase != null) {
            Path handbookPath = knowledgeBase.handbookPath();
            if (handbookPath != null && Files.exists(handbookPath)) {
              baseLogDir = handbookPath.resolve("logs");
              log.debug("Using handbook logs directory: {}", baseLogDir);
            } else {
              // Priority 4: Default to production mode location
              baseLogDir = Paths.get("/var/log/onemcp");
              log.debug("Using default production logging directory: {}", baseLogDir);
            }
          } else {
            // Priority 4: Default to production mode location
            baseLogDir = Paths.get("/var/log/onemcp");
            log.debug("Using default production logging directory: {}", baseLogDir);
          }
        } catch (Exception e) {
          log.debug("Could not determine handbook logs directory: {}", e.getMessage());
          // Priority 4: Default to production mode location
          baseLogDir = Paths.get("/var/log/onemcp");
          log.debug("Using default production logging directory: {}", baseLogDir);
        }
      }
    }

    // Reports go in {baseLogDir}/reports/
    Path reportsDir = baseLogDir.resolve("reports");

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
      KnowledgeBase knowledgeBase = oneMcp.knowledgeBase();
      if (knowledgeBase != null) {
        Path handbookPath = knowledgeBase.handbookPath();
        if (handbookPath != null) {
          return handbookPath.toString();
        }
      }
    } catch (Exception e) {
      log.debug("Could not get handbook path from KnowledgeBase: {}", e.getMessage());
    }
    
    return null;
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
      String timestamp = Instant.now().atOffset(ZoneOffset.UTC).format(
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSSSSS'Z'"));
      String filename = "execution-" + timestamp + ".txt";
      Path reportPath = reportsDirectory.resolve(filename);
      executionReportPaths.put(executionId, reportPath);

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
    try {
      if (reportModeEnabled) {
        List<ExecutionEvent> events = executionEvents.get(executionId);
        Path reportPath = executionReportPaths.get(executionId);

        if (events != null && reportPath != null) {
          // Add completion event
          events.add(
              new ExecutionEvent(
                  "execution_complete",
                  executionId,
                  Instant.now().toString(),
                  Map.of("durationMs", durationMs, "success", success)));

          // Generate and write report
          String reportContent = generateTextReport(executionId, events, durationMs, success);
          Files.writeString(reportPath, reportContent);

          String reportPathStr = reportPath.toString();
          currentReportPath.set(reportPathStr);
          log.info("Execution completed: {} (report: {})", executionId, reportPath);
          
          // Clean up events and paths, but keep currentReportPath until it's retrieved
          executionEvents.remove(executionId);
          executionReportPaths.remove(executionId);
          currentExecutionId.remove();
          // Note: currentReportPath is NOT removed here - it will be removed when retrieved
          
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
      executionReportPaths.remove(executionId);
      currentExecutionId.remove();
      // currentReportPath is intentionally NOT removed here - it will be cleared after retrieval
    }

    return null;
  }

  /**
   * Set the execution ID in thread-local for background threads.
   * This allows background threads to log events to the same execution.
   *
   * @param executionId the execution ID to set
   */
  public void setExecutionId(String executionId) {
    if (executionId != null) {
      currentExecutionId.set(executionId);
    }
  }

  /**
   * Get the report path for the current execution (from thread-local).
   * After retrieval, the thread-local is cleared.
   *
   * @return report path or null if not available
   */
  public String getCurrentReportPath() {
    String path = currentReportPath.get();
    if (path != null) {
      // Clear after retrieval to prevent stale data
      currentReportPath.remove();
    }
    return path;
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
    StringBuilder sb = new StringBuilder();

    // Header
    sb.append("╔══════════════════════════════════════════════════════════════════════════════╗\n");
    sb.append("║                          EXECUTION REPORT                                    ║\n");
    sb.append("╚══════════════════════════════════════════════════════════════════════════════╝\n");
    sb.append("\n");

    // Find start event for timestamp
    String startTimestamp = events.stream()
        .filter(e -> "execution_started".equals(e.type))
        .findFirst()
        .map(e -> e.timestamp)
        .orElse(Instant.now().toString());

    sb.append("  Timestamp: ").append(startTimestamp).append("\n");
    sb.append("  Duration:  ").append(durationMs).append("ms (").append(durationMs / 1000.0)
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
    long errors = events.stream()
        .filter(e -> "api_call_error".equals(e.type))
        .count();

    sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    sb.append("│ EXECUTION SUMMARY                                                            │\n");
    sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
    sb.append("\n");
    sb.append("  Status:              [").append(success ? "SUCCESS" : "FAILED").append("]\n");
    sb.append("  API Calls:           ").append(apiCalls).append("\n");
    sb.append("  Errors:              ").append(errors).append("\n");
    sb.append("\n");

    // LLM and API Calls
    int llmCallNum = 1;
    int apiCallNum = 1;
    long totalPromptTokens = 0;
    long totalCompletionTokens = 0;
    long totalLLMDuration = 0;
    
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
        
        String phaseStr = (phase != null && !phase.toString().equals("unknown")) ? phase.toString() : "?";
        // Track phase counts and append retry number if > 1
        int phaseCount = phaseCounts.getOrDefault(phaseStr, 0) + 1;
        phaseCounts.put(phaseStr, phaseCount);
        if (phaseCount > 1) {
          phaseStr = phaseStr + "#" + phaseCount;
        }
        // Format: "  LLM Call N (phase):    DURATIONms | Tokens: X+Y=Z"
        // Use fixed width for phase to ensure alignment
        String callLabel = String.format("  LLM Call %d (%-10s):", llmCallNum++, phaseStr);
        String durationStr = dur > 0 ? String.format("%6dms", dur) : "   N/A";
        // Compact token display: Tokens: 23432+233=23665
        String tokenStr = String.format("Tokens: %d+%d=%d", promptT, completionT, promptT + completionT);
        sb.append(String.format("%-30s %8s | %s", callLabel, durationStr, tokenStr)).append("\n");
      }
    }
    
    // API Calls
    for (ExecutionEvent event : events) {
      if ("api_call".equals(event.type)) {
        Object duration = event.data.get("durationMs");
        Object url = event.data.get("url");
        if (duration != null) {
          String apiLabel = String.format("  API Call %d:", apiCallNum++);
          String durationStr = duration instanceof Number 
              ? String.format("%6dms", ((Number) duration).longValue()) 
              : "   N/A";
          String urlStr = url != null ? url.toString() : "";
          sb.append(String.format("%-20s %8s   %s", apiLabel, durationStr, urlStr)).append("\n");
        }
      }
    }
    
    if (llmCallNum > 1) {
      sb.append("\n");
      sb.append("  Total LLM Duration:  ").append(String.format("%6d", totalLLMDuration)).append("ms\n");
      if (totalPromptTokens > 0 || totalCompletionTokens > 0) {
        sb.append("  Total Tokens:        ").append(totalPromptTokens).append("+").append(totalCompletionTokens).append("=").append(totalPromptTokens + totalCompletionTokens).append("\n");
      } else {
        sb.append("  Total Tokens:        0\n");
      }
    } else if (llmCallNum == 1 && apiCallNum == 1) {
      sb.append("\n");
      sb.append("  No LLM or API calls recorded\n");
    }
    sb.append("\n");

    // Normalized Prompt Schema (Background)
    boolean hasNormalizedSchema = false;
    long normalizationDuration = 0;
    for (ExecutionEvent event : events) {
      if ("normalized_prompt_schema".equals(event.type)) {
        hasNormalizedSchema = true;
        Object duration = event.data.get("durationMs");
        if (duration instanceof Number) {
          normalizationDuration = ((Number) duration).longValue();
        }
        break;
      }
    }
    
    if (hasNormalizedSchema) {
      sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
      sb.append("│ PROMPT SCHEMA (Background)                                                   │\n");
      sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
      sb.append("\n");
      
      for (ExecutionEvent event : events) {
        if ("normalized_prompt_schema".equals(event.type)) {
          Object schema = event.data.get("schema");
          if (schema != null && !schema.toString().trim().isEmpty()) {
            String schemaStr = schema.toString();
            // Check if this is an error message (dictionary not found, etc.)
            try {
              com.fasterxml.jackson.databind.JsonNode jsonNode = JacksonUtility.getJsonMapper().readTree(schemaStr);
              if (jsonNode.has("error")) {
                // This is an error message, display it clearly
                String error = jsonNode.get("error").asText();
                String reason = jsonNode.has("reason") ? jsonNode.get("reason").asText() : "unknown";
                sb.append("  Normalization skipped: ").append(error).append("\n");
                sb.append("  Reason: ").append(reason).append("\n");
              } else {
                // This is a valid schema, pretty-print it
                Object parsed = JacksonUtility.getJsonMapper().readValue(schemaStr, Object.class);
                schemaStr = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(parsed);
                String[] lines = schemaStr.split("\n");
                for (String line : lines) {
                  sb.append("  ").append(line).append("\n");
                }
              }
            } catch (Exception e) {
              // Not JSON or parsing failed, use as-is
              String[] lines = schemaStr.split("\n");
              for (String line : lines) {
                sb.append("  ").append(line).append("\n");
              }
            }
          } else {
            sb.append("  [No normalized schema captured]\n");
          }
          sb.append("\n");
          if (normalizationDuration > 0) {
            sb.append("  Background normalization latency: ").append(normalizationDuration).append("ms\n");
          }
          sb.append("\n");
          break; // Only show first normalized schema
        }
      }
    } else {
      sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
      sb.append("│ NORMALIZED PROMPT SCHEMA (Background)                                        │\n");
      sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
      sb.append("\n");
      sb.append("  [No normalized prompt schema recorded]\n");
      sb.append("\n");
    }

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
        if (completionTokens instanceof Number) completionT = ((Number) completionTokens).longValue();
        
        // Skip events with 0 tokens (likely duplicate/fallback events)
        if (promptT == 0 && completionT == 0) {
          continue; // Skip this event
        }
        
        String phaseStr = (phase != null && !phase.toString().equals("unknown")) ? phase.toString() : "?";
        // Track phase counts and append retry number if > 1
        int phaseCount = phaseCountsDetailed.getOrDefault(phaseStr, 0) + 1;
        phaseCountsDetailed.put(phaseStr, phaseCount);
        if (phaseCount > 1) {
          phaseStr = phaseStr + "#" + phaseCount;
        }
        String callHeader = "LLM Call " + llmInteractionNum + " (" + phaseStr + ")";
        
        // Box header for this LLM call
        sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
        sb.append("│ ").append(String.format("%-76s", callHeader)).append(" │\n");
        sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
        sb.append("\n");
        
        // Show duration and tokens as regular content below header
        StringBuilder details = new StringBuilder();
        if (duration != null) {
          details.append("Duration: ").append(duration).append("ms");
        }
        if (promptT > 0 || completionT > 0) {
          if (details.length() > 0) details.append(" | ");
          details.append("Tokens: ").append(promptT).append("+").append(completionT).append("=").append(promptT + completionT);
        }
        if (details.length() > 0) {
          sb.append("  ").append(details.toString()).append("\n");
          sb.append("\n");
        }
        
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
            sb.append("┌─ INPUT ────────────────────────────────────────────────────────────────────────\n");
            sb.append("│ [Same as previous LLM call]\n");
            sb.append("\n");
          } else {
            sb.append("┌─ INPUT ────────────────────────────────────────────────────────────────────────\n");
            String messagesStr = currentInputMessages;
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
          sb.append("┌─ INPUT ────────────────────────────────────────────────────────────────────────\n");
          sb.append("│ [No input messages captured]\n");
          sb.append("\n");
        }

        if (response != null && !response.toString().trim().isEmpty()) {
          sb.append("┌─ OUTPUT (").append(callHeader).append(") ────────────────────────────────────────────────────────────────\n");
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
          sb.append("┌─ OUTPUT (").append(callHeader).append(") ────────────────────────────────────────────────────────────────\n");
          sb.append("│ [No response text captured]\n");
          sb.append("\n");
        }
      }
    }
    sb.append("\n");

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

        String apiHeader = "API Call " + apiCallNum2;
        
        // Box header for this API call
        sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
        sb.append("│ ").append(String.format("%-76s", apiHeader)).append(" │\n");
        sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
        sb.append("\n");
        
        // Show method, URL, status, and duration as regular content
        StringBuilder details = new StringBuilder();
        if (method != null) {
          details.append("Method: ").append(method);
        }
        if (url != null) {
          if (details.length() > 0) details.append(" | ");
          details.append("URL: ").append(url);
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
            reqBodyStr = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(parsed);
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
            respBodyStr = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(parsed);
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
        StringBuilder curlCmd = new StringBuilder("curl -X ").append(method != null ? method : "GET");
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
          // Wrap long curl body lines
          if (escapedBody.length() > 70) {
            curlCmd.append("      -d '").append(escapedBody.substring(0, Math.min(70, escapedBody.length()))).append("...'\n");
          } else {
            curlCmd.append("      -d '").append(escapedBody).append("'\n");
          }
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

    // Execution Plan
    sb.append("┌──────────────────────────────────────────────────────────────────────────────┐\n");
    sb.append("│ EXECUTION PLAN                                                               │\n");
    sb.append("└──────────────────────────────────────────────────────────────────────────────┘\n");
    sb.append("\n");

    boolean hasExecutionPlan = false;
    for (ExecutionEvent event : events) {
      if ("execution_plan".equals(event.type)) {
        hasExecutionPlan = true;
        Object plan = event.data.get("plan");
        if (plan != null && !plan.toString().trim().isEmpty()) {
          String planStr = plan.toString();
          // Try to pretty-print JSON
          try {
            Object parsed = JacksonUtility.getJsonMapper().readValue(planStr, Object.class);
            planStr = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(parsed);
          } catch (Exception e) {
            // Not JSON, use as-is
          }
          String[] lines = planStr.split("\n");
          for (String line : lines) {
            sb.append("  ").append(line).append("\n");
          }
        } else {
          sb.append("  [No execution plan captured]\n");
        }
        sb.append("\n");
        break; // Only show first execution plan
      }
    }
    if (!hasExecutionPlan) {
      sb.append("  [No execution plan recorded]\n");
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
            responseStr = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(parsed);
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
            resultStr = JacksonUtility.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(parsed);
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
        events.add(new ExecutionEvent("llm_inference_complete", executionId, Instant.now().toString(), data));
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
            msgBuilder.append("[").append(msg.role()).append("] ").append(msg.content()).append("\n");
          }
        }
        events.add(
            new ExecutionEvent(
                "llm_input_messages",
                executionId,
                Instant.now().toString(),
                Map.of("messages", msgBuilder.toString().trim())));
        log.debug("LLM input messages logged (count: {}, executionId: {})", 
            messages != null ? messages.size() : 0, executionId);
      } else {
        log.warn("Cannot log LLM input messages: events list is null for executionId: {}", executionId);
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
                    "toolName", toolName != null ? toolName : "unknown",
                    "arguments", argsJson)));
        log.debug("Tool call logged: {} (executionId: {})", toolName, executionId);
      } else {
        log.warn("Cannot log tool call: events list is null for executionId: {} (tool: {})", 
            executionId, toolName);
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
            log.debug("Tool output added to tool_call event: {} (executionId: {})", toolName, executionId);
            break;
          }
        }
        if (!found) {
          log.warn("Could not find tool_call event for tool: {} (executionId: {})", toolName, executionId);
        }
      } else {
        log.warn("Cannot log tool output: events list is null for executionId: {} (tool: {})", 
            executionId, toolName);
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
        events.add(new ExecutionEvent("api_call_error", executionId, Instant.now().toString(), data));
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
        events.add(new ExecutionEvent("code_generation", executionId, Instant.now().toString(), data));
      }
    }

    log.info("Code generation: {} (success: {})", code != null ? code.length() : 0, success);
  }

  public void logExecutionPlan(String planJson) {
    String executionId = currentExecutionId.get();
    if (executionId == null) return;

    if (reportModeEnabled) {
      List<ExecutionEvent> events = executionEvents.get(executionId);
      if (events != null) {
        Map<String, Object> data = new HashMap<>();
        data.put("plan", planJson != null ? planJson : "");
        events.add(new ExecutionEvent("execution_plan", executionId, Instant.now().toString(), data));
      }
    }

    log.debug("Execution plan logged (length: {})", planJson != null ? planJson.length() : 0);
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
                Map.of("stepId", stepId != null ? stepId : "", "result", result != null ? result.toString() : "")));
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

  public void logNormalizedPromptSchemaForExecution(String normalizedSchemaJson, long durationMs, String executionId) {
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
                "normalized_prompt_schema",
                executionId,
                Instant.now().toString(),
                data));
        log.debug("Normalized prompt schema logged (background, {}ms, executionId: {})", durationMs, executionId);
      } else {
        log.warn("Cannot log normalized prompt schema: events list is null for executionId: {}", executionId);
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
          String assignmentResultJson = JacksonUtility.getJsonMapper().writeValueAsString(assignmentResult);
          Map<String, Object> data = new HashMap<>();
          data.put("assignmentResult", assignmentResultJson);
          events.add(
              new ExecutionEvent(
                  "assignment_result",
                  executionId,
                  Instant.now().toString(),
                  data));
          log.debug("AssigmentResult logged (executionId: {})", executionId);
        } catch (Exception e) {
          log.warn("Failed to serialize AssigmentResult to JSON (executionId: {}): {}", executionId, e.getMessage());
        }
      } else {
        log.warn("Cannot log AssigmentResult: events list is null for executionId: {}", executionId);
      }
    } else {
      log.debug("Report mode disabled, skipping AssigmentResult logging");
    }
  }

  /** Internal data structure for execution events. */
  public static class ExecutionEvent {
    public final String type;
    public final String executionId;
    public final String timestamp;
    public final Map<String, Object> data;

    public ExecutionEvent(String type, String executionId, String timestamp, Map<String, Object> data) {
      this.type = type;
      this.executionId = executionId;
      this.timestamp = timestamp;
      this.data = data != null ? new HashMap<>(data) : new HashMap<>();
    }
  }
}
