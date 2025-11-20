package com.gentoro.onemcp.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.context.KnowledgeBase;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inference logger that handles both production-safe logging and detailed execution reports.
 *
 * <p>This logger handles two separate concerns:
 * <ol>
 *   <li><b>Production Logging</b>: Always enabled, writes to standard SLF4J loggers
 *       (rotated production logs). Safe for production - no code, no sensitive data.</li>
 *   <li><b>Report Logging</b>: Optional feature (report mode), generates detailed execution reports.
 *       Location configurable:
 *       <ul>
 *         <li>CLI/Handbook mode: Reports in handbook directory (convenient for exploration)</li>
 *         <li>Production mode: Reports in separate directory (e.g., /var/log/onemcp/reports)</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p>Production logs are written to standard SLF4J loggers, making them compatible with
 * log aggregation systems (Datadog, Splunk, ELK, etc.).
 * 
 * <p>All report mode conditionals are handled internally - callers never need to check report mode.
 */
public class InferenceLogger {
  private static final Logger log = LoggerFactory.getLogger(InferenceLogger.class);
  
  private final OneMcp oneMcp;
  private final boolean reportModeEnabled;
  
  // Report mode fields (only used if reportModeEnabled is true)
  private final Path reportsDir; // null if report mode disabled
  private final ObjectMapper objectMapper; // null if report mode disabled
  // In-memory event storage: executionId -> list of events
  private final Map<String, List<ExecutionEvent>> executionEvents = new java.util.concurrent.ConcurrentHashMap<>();
  // Report path storage: executionId -> report path
  private final Map<String, Path> executionReportPaths = new java.util.concurrent.ConcurrentHashMap<>();
  private String currentExecutionId = null;
  private Path currentReportPath = null; // Report path determined at execution start
  private static final ThreadLocal<String> currentPhase = new ThreadLocal<>();

  public InferenceLogger(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    
    // Check if report mode is enabled via environment variable or config
    String reportMode = System.getenv("ONEMCP_REPORTS_ENABLED");
    if (reportMode == null) {
      // Check config file
      reportMode = oneMcp.configuration().getString("reports.enabled", null);
      
      // Auto-enable for handbook mode (CLI usage)
      if (reportMode == null) {
        try {
          KnowledgeBase knowledgeBase = oneMcp.knowledgeBase();
          if (knowledgeBase != null && knowledgeBase.handbookPath() != null) {
            // CLI/Handbook mode: auto-enable detailed reports
            reportMode = "true";
            log.info("Report mode auto-enabled for CLI/handbook mode");
          } else {
            // Production mode: disable by default (use production-safe logging only)
            reportMode = "false";
          }
        } catch (Exception e) {
          // If we can't determine, default to false for safety
          reportMode = "false";
        }
      }
    }
    this.reportModeEnabled = "true".equalsIgnoreCase(reportMode);
    
    // Determine logging directory (single directory for all logging)
    String logDir = determineLoggingDirectory();
    
    // Determine report directory (subdirectory within logging directory)
    String reportDir = determineReportDirectory();
    
    // Initialize report mode fields if enabled
    Path reportsDirTemp = null;
    ObjectMapper objectMapperTemp = null;
    
    if (reportModeEnabled) {
      try {
        reportsDirTemp = Path.of(reportDir);
        Files.createDirectories(reportsDirTemp);
        
        // Use shared Jackson utility for consistency
        objectMapperTemp = JacksonUtility.getJsonMapper();
        
        log.info("Logging directory: {}", logDir);
        log.info("Report mode enabled - execution reports will be generated at: {}", reportDir);
        log.info("Production logs: {}/onemcp.log, Reports: {}", logDir, reportDir);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize InferenceLogger report mode", e);
      }
    } else {
      log.debug("Report mode disabled - only production logs will be generated at: {}", logDir);
    }
    
    this.reportsDir = reportsDirTemp;
    this.objectMapper = objectMapperTemp;
  }

  /**
   * Determine the report directory location.
   * Reports are stored in {logging_dir}/reports/ subdirectory.
   */
  private String determineReportDirectory() {
    // Get the base logging directory
    String logDir = determineLoggingDirectory();
    
    // Reports always go in a reports/ subdirectory
    return java.nio.file.Path.of(logDir).resolve("reports").toString();
  }

  /**
   * Determine the base logging directory.
   * This is where both production logs and reports are stored.
   * 
   * @throws RuntimeException if the directory cannot be created or is not writable
   */
  private String determineLoggingDirectory() {
    String logDir = null;
    
    // Check explicit configuration first
    logDir = System.getenv("ONEMCP_LOG_DIR");
    if (logDir != null && !logDir.isEmpty()) {
      return validateAndCreateDirectory(logDir);
    }
    
    // Check config file
    logDir = oneMcp.configuration().getString("logging.directory", null);
    if (logDir != null && !logDir.isEmpty()) {
      return validateAndCreateDirectory(logDir);
    }
    
    // Check if we're in handbook mode (CLI usage)
    try {
      KnowledgeBase knowledgeBase = oneMcp.knowledgeBase();
      if (knowledgeBase != null) {
        java.nio.file.Path handbookPath = knowledgeBase.handbookPath();
        if (handbookPath != null) {
          // CLI/Handbook mode: use handbook/logs directory
          java.nio.file.Path logsPath = handbookPath.resolve("logs");
          return validateAndCreateDirectory(logsPath.toString());
        }
      }
    } catch (Exception e) {
      log.debug("Could not determine handbook path, using default logging location", e);
    }
    
    // Production default: /var/log/onemcp
    return validateAndCreateDirectory("/var/log/onemcp");
  }

  /**
   * Validate that a directory exists (or can be created) and is writable.
   * 
   * @param dirPath The directory path to validate
   * @return The validated directory path
   * @throws RuntimeException if the directory cannot be created or is not writable
   */
  private String validateAndCreateDirectory(String dirPath) {
    java.io.File dir = new java.io.File(dirPath);
    
    // Try to create directory if it doesn't exist
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new RuntimeException(
            String.format("Failed to create logging directory: %s. " +
                "Please create it manually or set ONEMCP_LOG_DIR environment variable.", dirPath));
      }
    }
    
    // Verify it's actually a directory
    if (!dir.isDirectory()) {
      throw new RuntimeException(
          String.format("Logging path exists but is not a directory: %s. " +
              "Please remove it or set ONEMCP_LOG_DIR to a different location.", dirPath));
    }
    
    // Verify directory is writable
    if (!dir.canWrite()) {
      throw new RuntimeException(
          String.format("Logging directory is not writable: %s. " +
              "Please check permissions or set ONEMCP_LOG_DIR environment variable.", dirPath));
    }
    
    return dirPath;
  }

  /**
   * Check if report mode is enabled.
   */
  public boolean isReportModeEnabled() {
    return reportModeEnabled;
  }

  /**
   * Set the current execution phase for this thread.
   */
  public void setCurrentPhase(String phase) {
    currentPhase.set(phase);
  }

  /**
   * Get the current execution phase for this thread.
   */
  public String getCurrentPhase() {
    return currentPhase.get();
  }

  /**
   * Start tracking a new execution.
   * Handles both production logging and report logging internally.
   * Returns the report path that will be used for this execution (determined pre-operation).
   */
  public String startExecution(String executionId, String userQuery) {
    String reportPath = null;
    // Report mode: initialize in-memory event storage
    if (reportModeEnabled) {
      // Generate report filename pre-operation (before execution starts)
      String fileTimestamp = Instant.now().toString().replace(":", "-");
      this.currentReportPath = reportsDir.resolve("execution-" + fileTimestamp + ".txt");
      reportPath = this.currentReportPath.toString();
      
      // Initialize event list for this execution
      this.currentExecutionId = executionId;
      executionEvents.put(executionId, new ArrayList<>());
      // Store report path for this execution
      executionReportPaths.put(executionId, this.currentReportPath);
      
      ExecutionEvent event =
          new ExecutionEvent(
              "execution_started",
              executionId,
              Map.of("userQuery", userQuery != null ? userQuery : ""));
      appendEvent(event);
    }
    
    // Always log execution start in production-safe format
    log.info("Execution started: executionId={}, queryLength={}", 
        executionId, 
        userQuery != null ? userQuery.length() : 0);
    
    if (reportPath != null) {
      log.info("Execution report will be written to: {}", reportPath);
    }
    
    // Log query hash for traceability (not the actual query for privacy)
    if (userQuery != null && !userQuery.isEmpty()) {
      int queryHash = userQuery.hashCode();
      log.debug("Query hash: {}", queryHash);
    }
    
    return reportPath;
  }

  /**
   * Log LLM inference start.
   * Handles both production logging and report logging internally.
   */
  public void logLlmInferenceStart(int messageCount, int toolCount) {
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      String phase = getCurrentPhase();
      ExecutionEvent event =
          new ExecutionEvent(
              "llm_inference_start",
              currentExecutionId,
              Map.of(
                  "phase", phase != null ? phase : "unknown",
                  "messageCount", messageCount,
                  "toolCount", toolCount));
      appendEvent(event);
    }
    
    // Always log in production-safe format
    log.debug("LLM inference starting: messages={}, tools={}", messageCount, toolCount);
  }

  /**
   * Log LLM inference completion.
   * Handles both production logging and report logging internally.
   */
  public void logLlmInferenceComplete(long durationMs, Integer tokens, String response) {
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      String phase = getCurrentPhase();
      // Truncate very long responses to avoid huge memory usage
      String responsePreview = response;
      if (response != null && response.length() > 1000) {
        responsePreview = response.substring(0, 1000) + "... [truncated]";
      }

      ExecutionEvent event =
          new ExecutionEvent(
              "llm_inference_complete",
              currentExecutionId,
              Map.of(
                  "phase", phase != null ? phase : "unknown",
                  "durationMs", durationMs,
                  "tokens", tokens != null ? tokens : 0,
                  "responsePreview", responsePreview != null ? responsePreview : ""));
      appendEvent(event);
    }
    
    // Always log in production-safe format (metrics only, not content)
    log.info("LLM inference complete: duration={}ms, tokens={}, responseLength={}", 
        durationMs, 
        tokens != null ? tokens : 0,
        response != null ? response.length() : 0);
    
    // Only log response preview at TRACE level (for debugging)
    if (log.isTraceEnabled() && response != null && response.length() > 0) {
      String preview = response.length() > 100 
          ? response.substring(0, 100) + "..." 
          : response;
      log.trace("LLM response preview: {}", preview);
    }
  }

  /**
   * Log a tool call.
   * Handles both production logging and report logging internally.
   */
  public void logToolCall(String toolName, Map<String, Object> arguments) {
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      // Truncate large argument objects
      Map<String, Object> argsPreview = arguments;
      if (arguments != null && arguments.toString().length() > 500) {
        argsPreview = Map.of("_truncated", "true", "_size", arguments.size());
      }

      ExecutionEvent event =
          new ExecutionEvent(
              "tool_call",
              currentExecutionId,
              Map.of("toolName", toolName != null ? toolName : "unknown", "arguments", argsPreview != null ? argsPreview : Map.of()));
      appendEvent(event);
    }
    
    // Always log in production-safe format
    int argCount = arguments != null ? arguments.size() : 0;
    log.debug("Tool call: tool={}, argCount={}", toolName, argCount);
    
    // Log argument keys only (not values) at TRACE level
    if (log.isTraceEnabled() && arguments != null) {
      log.trace("Tool arguments keys: {}", arguments.keySet());
    }
  }

  /**
   * Log tool output/result.
   * Handles both production logging and report logging internally.
   * Truncates output and records the number of lines truncated.
   */
  public void logToolOutput(String toolName, String output) {
    // Report mode: update the most recent tool_call event with output
    if (reportModeEnabled && currentExecutionId != null) {
      List<ExecutionEvent> events = getEvents(currentExecutionId);
      // Find the most recent tool_call event for this tool
      ExecutionEvent toolEvent = null;
      for (int i = events.size() - 1; i >= 0; i--) {
        ExecutionEvent event = events.get(i);
        if ("tool_call".equals(event.type) && toolName.equals(event.data.get("toolName"))) {
          toolEvent = event;
          break;
        }
      }
      
      if (toolEvent != null) {
        // Calculate truncation info
        int maxLines = 50; // Show first 50 lines
        String[] lines = output != null ? output.split("\n", -1) : new String[0];
        int totalLines = lines.length;
        boolean truncated = totalLines > maxLines;
        
        String truncatedOutput = output;
        int linesTruncated = 0;
        if (truncated) {
          StringBuilder truncatedBuilder = new StringBuilder();
          for (int i = 0; i < maxLines && i < lines.length; i++) {
            if (i > 0) truncatedBuilder.append("\n");
            truncatedBuilder.append(lines[i]);
          }
          truncatedOutput = truncatedBuilder.toString();
          linesTruncated = totalLines - maxLines;
        }
        
        // Update the event with output data
        Map<String, Object> updatedData = new java.util.HashMap<>(toolEvent.data);
        updatedData.put("output", truncatedOutput);
        updatedData.put("outputTruncated", truncated);
        updatedData.put("outputTotalLines", totalLines);
        updatedData.put("outputLinesTruncated", linesTruncated);
        toolEvent.data = updatedData;
      }
    }
    
    // Always log in production-safe format (metadata only, not content)
    int outputLength = output != null ? output.length() : 0;
    int lineCount = output != null ? output.split("\n", -1).length : 0;
    log.debug("Tool output: tool={}, length={}, lines={}", toolName, outputLength, lineCount);
  }

  /**
   * Log an API call.
   * Handles both production logging and report logging internally.
   */
  public void logApiCall(
      String method,
      String url,
      Map<String, String> requestHeaders,
      String requestBody,
      int statusCode,
      Map<String, String> responseHeaders,
      String responseBody,
      long durationMs) {
    
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      // Truncate very long bodies
      String requestBodyPreview = requestBody;
      if (requestBody != null && requestBody.length() > 2000) {
        requestBodyPreview = requestBody.substring(0, 2000) + "... [truncated]";
      }

      String responseBodyPreview = responseBody;
      if (responseBody != null && responseBody.length() > 5000) {
        responseBodyPreview = responseBody.substring(0, 5000) + "... [truncated]";
      }

      ExecutionEvent event =
          new ExecutionEvent(
              "api_call",
              currentExecutionId,
              Map.of(
                  "method", method != null ? method : "GET",
                  "url", url != null ? url : "",
                  "requestHeaders", requestHeaders != null ? requestHeaders : Map.of(),
                  "requestBody", requestBodyPreview != null ? requestBodyPreview : "",
                  "statusCode", statusCode,
                  "responseHeaders", responseHeaders != null ? responseHeaders : Map.of(),
                  "responseBody", responseBodyPreview != null ? responseBodyPreview : "",
                  "durationMs", durationMs));
      appendEvent(event);
    }
    
    // Always log in production-safe format (metrics only, not bodies)
    log.info("API call: method={}, url={}, status={}, duration={}ms, requestSize={}, responseSize={}",
        method,
        sanitizeUrl(url),
        statusCode,
        durationMs,
        requestBody != null ? requestBody.length() : 0,
        responseBody != null ? responseBody.length() : 0);
    
    // Log at WARN level if status indicates error
    if (statusCode >= 400) {
      log.warn("API call failed: method={}, url={}, status={}, duration={}ms",
          method, sanitizeUrl(url), statusCode, durationMs);
    }
    
    // Only log bodies at TRACE level (for debugging)
    if (log.isTraceEnabled()) {
      if (requestBody != null && requestBody.length() > 0) {
        log.trace("API request body: {}", truncate(requestBody, 500));
      }
      if (responseBody != null && responseBody.length() > 0) {
        log.trace("API response body: {}", truncate(responseBody, 500));
      }
    }
  }

  /**
   * Log an API call error (timeout, connection error, etc.).
   * Handles both production logging and report logging internally.
   */
  public void logApiCallError(
      String method,
      String url,
      Map<String, String> requestHeaders,
      String requestBody,
      int statusCode,
      String errorType,
      String errorMessage,
      long durationMs) {
    
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      // Truncate very long bodies
      String requestBodyPreview = requestBody;
      if (requestBody != null && requestBody.length() > 2000) {
        requestBodyPreview = requestBody.substring(0, 2000) + "... [truncated]";
      }

      ExecutionEvent event =
          new ExecutionEvent(
              "api_call_error",
              currentExecutionId,
              Map.of(
                  "method", method != null ? method : "GET",
                  "url", url != null ? url : "",
                  "requestHeaders", requestHeaders != null ? requestHeaders : Map.of(),
                  "requestBody", requestBodyPreview != null ? requestBodyPreview : "",
                  "statusCode", statusCode,
                  "errorType", errorType != null ? errorType : "Unknown Error",
                  "errorMessage", errorMessage != null ? errorMessage : "",
                  "durationMs", durationMs));
      appendEvent(event);
    }
    
    // Always log in production-safe format
    log.warn("API call error: method={}, url={}, errorType={}, errorMessage={}, duration={}ms",
        method, sanitizeUrl(url), errorType, errorMessage, durationMs);
  }

  /**
   * Log code generation.
   * Handles both production logging and report logging internally.
   */
  public void logCodeGeneration(String code) {
    // Report mode: add event to in-memory storage (includes full code)
    if (reportModeEnabled && currentExecutionId != null) {
      // Don't truncate code - reports need the full generated code
      ExecutionEvent event =
          new ExecutionEvent(
              "code_generation",
              currentExecutionId,
              Map.of("code", code != null ? code : ""));
      appendEvent(event);
    }
    
    // Always log in production-safe format (metadata only, never the code itself)
    int codeSize = code != null ? code.length() : 0;
    int lineCount = code != null ? code.split("\n").length : 0;
    
    log.info("Code generated: size={} bytes, lines={}", codeSize, lineCount);
    
    // Extract class name if possible (safe to log)
    if (code != null && code.contains("class ")) {
      try {
        int classIdx = code.indexOf("class ");
        int start = classIdx + 6;
        int end = code.indexOf(" ", start);
        if (end == -1) end = code.indexOf("{", start);
        if (end > start) {
          String className = code.substring(start, end).trim();
          log.debug("Generated class: {}", className);
        }
      } catch (Exception e) {
        // Ignore parsing errors
      }
    }
    
    // NEVER log the actual code in production mode
    // This is a security/privacy concern - code may contain sensitive data
  }

  /**
   * Log an execution phase change.
   * Handles both production logging and report logging internally.
   */
  public void logPhaseChange(String phase) {
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      ExecutionEvent event =
          new ExecutionEvent(
              "phase_change",
              currentExecutionId,
              Map.of("phase", phase != null ? phase : "unknown"));
      appendEvent(event);
    }
    
    // Always log in production-safe format
    log.debug("Phase change: {}", phase);
  }

  /**
   * Log an error.
   * Handles both production logging and report logging internally.
   * Errors are ALWAYS logged regardless of mode.
   */
  public void logError(String message, Throwable throwable) {
    // Report mode: could add error logging to event logger if needed
    // Currently errors are only logged to production logs for safety
    
    // Always log errors
    if (throwable != null) {
      log.error("Execution error: {}", message, throwable);
    } else {
      log.error("Execution error: {}", message);
    }
  }

  /**
   * Log the final response from the orchestrator.
   * Handles both production logging and report logging internally.
   */
  public void logFinalResponse(String response) {
    // Report mode: add event to in-memory storage
    if (reportModeEnabled && currentExecutionId != null) {
      ExecutionEvent event =
          new ExecutionEvent(
              "final_response",
              currentExecutionId,
              Map.of("response", response != null ? response : ""));
      appendEvent(event);
    }
    
    // Always log in production-safe format (response length only)
    log.info("Final response generated: length={}", response != null ? response.length() : 0);
  }

  /**
   * Complete execution and generate report.
   * Handles both production logging and report logging internally.
   * Returns the report path if a report was generated.
   */
  public String completeExecution(String executionId, long durationMs, boolean success) {
    // Report mode: generate report
    String reportPath = null;
    if (reportModeEnabled) {
      // Use the executionId parameter, but also check currentExecutionId for compatibility
      String execIdToUse = (currentExecutionId != null && currentExecutionId.equals(executionId)) 
          ? currentExecutionId 
          : executionId;
      
      if (execIdToUse != null) {
        ExecutionEvent event =
            new ExecutionEvent("execution_complete", execIdToUse, Map.of());
        appendEvent(event);
        
        // Generate beautiful text report (uses currentReportPath set at startExecution)
        reportPath = generateTextReport(execIdToUse);
        
        // Clean up in-memory events for this execution
        executionEvents.remove(execIdToUse);
        executionReportPaths.remove(execIdToUse); // Clean up report path mapping
        if (currentExecutionId != null && currentExecutionId.equals(execIdToUse)) {
          this.currentExecutionId = null;
          this.currentReportPath = null; // Clear report path after generating report
        }
        // Clear thread-local phase
        currentPhase.remove();
      } else {
        log.warn("Cannot generate report: executionId is null");
      }
    }
    
    // Always log completion in production-safe format
    if (success) {
      log.info("Execution completed: executionId={}, duration={}ms, status=SUCCESS",
          executionId, durationMs);
    } else {
      log.warn("Execution completed: executionId={}, duration={}ms, status=FAILED",
          executionId, durationMs);
    }
    
    // Log report location for user visibility (only in report mode)
    if (reportPath != null) {
      log.info("================================================================================");
      log.info("Execution report: {}", reportPath);
      log.info("View with: cat {} | less", reportPath);
      log.info("Or: vi {}", reportPath);
      log.info("================================================================================");
    }
    
    return reportPath;
  }

  /**
   * Sanitize URL to remove sensitive query parameters.
   */
  private String sanitizeUrl(String url) {
    if (url == null) return "";
    
    // Remove query parameters (may contain sensitive data)
    int queryIdx = url.indexOf('?');
    if (queryIdx > 0) {
      return url.substring(0, queryIdx) + "?<params>";
    }
    
    return url;
  }

  /**
   * Truncate string for logging.
   */
  private String truncate(String str, int maxLength) {
    if (str == null) return "";
    if (str.length() <= maxLength) return str;
    return str.substring(0, maxLength) + "... [truncated]";
  }

  // ============================================================================
  // Report Mode Methods (only used when reportModeEnabled is true)
  // ============================================================================

  /**
   * Append an event to in-memory storage.
   */
  private void appendEvent(ExecutionEvent event) {
    if (!reportModeEnabled || event.executionId == null) {
      return;
    }
    executionEvents.computeIfAbsent(event.executionId, k -> new ArrayList<>()).add(event);
  }

  /**
   * Get all events for a specific execution from in-memory storage.
   */
  private List<ExecutionEvent> getEvents(String executionId) {
    if (!reportModeEnabled || executionId == null) {
      return new ArrayList<>();
    }
    return executionEvents.getOrDefault(executionId, new ArrayList<>());
  }

  /**
   * Generate beautiful plain text execution report.
   */
  private String generateTextReport(String executionId) {
    if (!reportModeEnabled || executionId == null || objectMapper == null) {
      if (!reportModeEnabled) {
        log.debug("Report generation skipped: report mode disabled");
      } else if (executionId == null) {
        log.debug("Report generation skipped: executionId is null");
      } else if (objectMapper == null) {
        log.debug("Report generation skipped: objectMapper is null");
      }
      return null;
    }
    try {
      List<ExecutionEvent> events = getEvents(executionId);
      if (events.isEmpty()) {
        log.warn("Report generation skipped: no events found for executionId={}", executionId);
        return null;
      }
      
      log.debug("Generating report for executionId={} with {} events", executionId, events.size());
      
      // Find key events
      ExecutionEvent startEvent = events.stream()
          .filter(e -> "execution_started".equals(e.type))
          .findFirst()
          .orElse(null);
      ExecutionEvent completeEvent = events.stream()
          .filter(e -> "execution_complete".equals(e.type))
          .findFirst()
          .orElse(null);
      
      // Calculate total duration
      long totalDurationMs = 0;
      String timestamp = "";
      if (startEvent != null && completeEvent != null) {
        try {
          Instant startTime = Instant.parse(startEvent.timestamp);
          Instant endTime = Instant.parse(completeEvent.timestamp);
          totalDurationMs = java.time.Duration.between(startTime, endTime).toMillis();
          timestamp = startEvent.timestamp;
        } catch (Exception e) {
          log.debug("Could not parse timestamps for duration calculation", e);
        }
      }
      
      // Collect events by type
      List<ExecutionEvent> codeEvents = events.stream()
          .filter(e -> "code_generation".equals(e.type))
          .toList();
      List<ExecutionEvent> apiEvents = events.stream()
          .filter(e -> "api_call".equals(e.type))
          .toList();
      List<ExecutionEvent> apiErrorEvents = events.stream()
          .filter(e -> "api_call_error".equals(e.type))
          .toList();
      List<ExecutionEvent> toolEvents = events.stream()
          .filter(e -> "tool_call".equals(e.type))
          .toList();
      List<ExecutionEvent> llmEvents = events.stream()
          .filter(e -> "llm_inference_complete".equals(e.type))
          .toList();
      List<ExecutionEvent> phaseEvents = events.stream()
          .filter(e -> "phase_change".equals(e.type))
          .toList();
      ExecutionEvent finalResponseEvent = events.stream()
          .filter(e -> "final_response".equals(e.type))
          .findFirst()
          .orElse(null);
      
      // Count errors (events with error status codes or API call errors)
      long errorCount = apiEvents.stream()
          .filter(e -> {
            Object statusCode = e.data.get("statusCode");
            if (statusCode != null) {
              int httpStatus = (int) statusCode;
              return httpStatus >= 400;
            }
            return false;
          })
          .count();
      errorCount += apiErrorEvents.size(); // Add API call errors (timeouts, connection errors, etc.)
      
      // Determine status
      String status = errorCount > 0 ? "[FAILED]" : "[SUCCESS]";
      if (completeEvent == null) {
        status = "[INCOMPLETE]";
      }
      
      // Build report content
      StringBuilder report = new StringBuilder();
      report.append("================================================================================\n");
      report.append("                                EXECUTION REPORT                                \n");
      report.append("================================================================================\n\n");
      
      // Timestamp and Duration
      report.append("Timestamp: ").append(timestamp).append("\n");
      report.append("Duration:  ").append(totalDurationMs).append("ms (")
          .append(String.format("%.2f", totalDurationMs / 1000.0)).append("s)\n\n");
      
      // EXECUTION SUMMARY
      report.append("--------------------------------------------------------------------------------\n");
      report.append("EXECUTION SUMMARY\n");
      report.append("--------------------------------------------------------------------------------\n\n");
      report.append("Status:              ").append(status).append("\n");
      report.append("Tool Calls:          ").append(toolEvents.size()).append("\n");
      report.append("API Calls:           ").append(apiEvents.size() + apiErrorEvents.size()).append("\n");
      report.append("Code Blocks:         ").append(codeEvents.size()).append("\n");
      report.append("Errors:              ").append(errorCount).append("\n\n");
      
      // TIMING BREAKDOWN
      report.append("--------------------------------------------------------------------------------\n");
      report.append("TIMING BREAKDOWN\n");
      report.append("--------------------------------------------------------------------------------\n\n");
      
      // Phase timings
      long totalPhaseTime = 0;
      for (ExecutionEvent phaseEvent : phaseEvents) {
        String phase = (String) phaseEvent.data.get("phase");
        // Find next phase or completion to calculate duration
        int phaseIndex = events.indexOf(phaseEvent);
        long phaseDuration = 0;
        if (phaseIndex < events.size() - 1) {
          try {
            Instant phaseStart = Instant.parse(phaseEvent.timestamp);
            ExecutionEvent nextEvent = events.get(phaseIndex + 1);
            Instant nextTime = Instant.parse(nextEvent.timestamp);
            phaseDuration = java.time.Duration.between(phaseStart, nextTime).toMillis();
            totalPhaseTime += phaseDuration;
          } catch (Exception e) {
            // Ignore
          }
        }
        report.append("  ").append(String.format("%-15s", phase + " phase:"))
            .append(String.format("%6d", phaseDuration)).append("ms\n");
      }
      
      // LLM inference timings
      long totalLlmTime = 0;
      for (int i = 0; i < llmEvents.size(); i++) {
        ExecutionEvent event = llmEvents.get(i);
        Object durationMs = event.data.get("durationMs");
        Object phase = event.data.get("phase");
        if (durationMs != null) {
          long duration = ((Number) durationMs).longValue();
          totalLlmTime += duration;
          String phaseStr = phase != null ? " (" + phase + ")" : "";
          report.append("  ").append(String.format("%-15s", "LLM Call " + (i + 1) + phaseStr + ":"))
              .append(String.format("%6d", duration)).append("ms\n");
        }
      }
      
      // API call timings (including errors)
      long totalApiTime = 0;
      int apiCallNum = 1;
      for (ExecutionEvent event : apiEvents) {
        Object durationMs = event.data.get("durationMs");
        Object url = event.data.get("url");
        if (durationMs != null) {
          long duration = ((Number) durationMs).longValue();
          totalApiTime += duration;
          String urlStr = url != null ? url.toString() : "";
          // Truncate URL for display
          if (urlStr.length() > 40) {
            urlStr = urlStr.substring(0, 37) + "...";
          }
          report.append("  ").append(String.format("%-15s", "API Call " + apiCallNum++ + ":"))
              .append(String.format("%6d", duration)).append("ms  ")
              .append(urlStr).append("\n");
        }
      }
      for (ExecutionEvent event : apiErrorEvents) {
        Object durationMs = event.data.get("durationMs");
        Object url = event.data.get("url");
        Object errorType = event.data.get("errorType");
        if (durationMs != null) {
          long duration = ((Number) durationMs).longValue();
          totalApiTime += duration;
          String urlStr = url != null ? url.toString() : "";
          // Truncate URL for display
          if (urlStr.length() > 35) {
            urlStr = urlStr.substring(0, 32) + "...";
          }
          String errorLabel = errorType != null ? " [" + errorType + "]" : " [ERROR]";
          report.append("  ").append(String.format("%-15s", "API Call " + apiCallNum++ + errorLabel + ":"))
              .append(String.format("%6d", duration)).append("ms  ")
              .append(urlStr).append("\n");
        }
      }
      
      report.append("  ").append(String.format("%-15s", "Total:"))
          .append(String.format("%6d", totalDurationMs)).append("ms\n\n");
      
      // USER QUERY
      report.append("--------------------------------------------------------------------------------\n");
      report.append("USER QUERY\n");
      report.append("--------------------------------------------------------------------------------\n\n");
      if (startEvent != null) {
        Object userQuery = startEvent.data.get("userQuery");
        if (userQuery != null) {
          report.append("  ").append(userQuery.toString().replace("\n", "\n  ")).append("\n\n");
        }
      }
      
      // RESPONSE
      report.append("--------------------------------------------------------------------------------\n");
      report.append("RESPONSE\n");
      report.append("--------------------------------------------------------------------------------\n\n");
      if (finalResponseEvent != null) {
        Object response = finalResponseEvent.data.get("response");
        if (response != null && !response.toString().isEmpty()) {
          report.append("  ").append(response.toString().replace("\n", "\n  ")).append("\n\n");
        }
      }
      
      // TOOL CALLS
      if (!toolEvents.isEmpty()) {
        report.append("--------------------------------------------------------------------------------\n");
        report.append("TOOL CALLS\n");
        report.append("--------------------------------------------------------------------------------\n\n");
        
        int toolNumber = 1;
        for (ExecutionEvent event : toolEvents) {
          report.append("Tool Call ").append(toolNumber++).append("\n");
          report.append("----------------------------------------\n\n");
          Object toolName = event.data.get("toolName");
          Object arguments = event.data.get("arguments");
          
          if (toolName != null) {
            report.append("Tool:     ").append(toolName).append("\n");
          }
          
          if (arguments != null && arguments instanceof Map && !((Map<?, ?>) arguments).isEmpty()) {
            report.append("Arguments:\n");
            @SuppressWarnings("unchecked")
            Map<String, Object> args = (Map<String, Object>) arguments;
            args.forEach((k, v) -> report.append("  ").append(k).append(": ").append(v).append("\n"));
          } else {
            report.append("Arguments: (none logged)\n");
          }
          
          // Show tool output if available
          Object output = event.data.get("output");
          Object outputTruncated = event.data.get("outputTruncated");
          Object outputTotalLines = event.data.get("outputTotalLines");
          Object outputLinesTruncated = event.data.get("outputLinesTruncated");
          
          if (output != null) {
            report.append("\nOutput:\n");
            String outputStr = output.toString();
            if (outputTruncated != null && Boolean.TRUE.equals(outputTruncated)) {
              report.append("  ").append(outputStr.replace("\n", "\n  ")).append("\n");
              if (outputLinesTruncated != null) {
                int linesTruncated = ((Number) outputLinesTruncated).intValue();
                int totalLines = outputTotalLines != null ? ((Number) outputTotalLines).intValue() : 0;
                report.append("\n  [Output truncated: showing first 50 of ")
                    .append(totalLines).append(" lines (")
                    .append(linesTruncated).append(" lines elided)]\n");
              }
            } else {
              report.append("  ").append(outputStr.replace("\n", "\n  ")).append("\n");
            }
          }
          
          report.append("\n");
        }
        report.append("--------------------------------------------------------------------------------\n\n");
      }
      
      // API CALLS (including errors)
      int totalApiCalls = apiEvents.size() + apiErrorEvents.size();
      if (totalApiCalls > 0) {
        report.append("--------------------------------------------------------------------------------\n");
        report.append("API CALLS\n");
        report.append("--------------------------------------------------------------------------------\n\n");
        
        int callNumber = 1;
        
        // Process regular API calls
        for (ExecutionEvent event : apiEvents) {
          report.append("API Call ").append(callNumber++).append("\n");
          report.append("----------------------------------------\n\n");
          report.append("Method:  ").append(event.data.get("method")).append("\n");
          report.append("URL:     ").append(event.data.get("url")).append("\n\n");
          
          Object statusCode = event.data.get("statusCode");
          if (statusCode != null) {
            int httpStatus = (int) statusCode;
            String statusText = httpStatus >= 200 && httpStatus < 300 ? "[SUCCESS]" : "[FAILED]";
            report.append("Status:  ").append(httpStatus).append(" ").append(statusText).append("\n\n");
          }
          
          Object requestHeaders = event.data.get("requestHeaders");
          if (requestHeaders != null && requestHeaders instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, String> headers = (Map<String, String>) requestHeaders;
            if (!headers.isEmpty()) {
              report.append("Headers:\n");
              headers.forEach((k, v) -> report.append("  ").append(k).append(": ").append(v).append("\n"));
              report.append("\n");
            }
          }
          
          Object requestBody = event.data.get("requestBody");
          if (requestBody != null && !requestBody.toString().isEmpty()) {
            report.append("Request Body:\n");
            String bodyStr = requestBody.toString();
            // Format JSON if possible
            if (bodyStr.trim().startsWith("{")) {
              try {
                bodyStr = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(objectMapper.readTree(bodyStr));
              } catch (Exception e) {
                // Use original if formatting fails
              }
            }
            report.append("  ").append(bodyStr.replace("\n", "\n  ")).append("\n\n");
          }
          
          if (statusCode != null) {
            Object responseBody = event.data.get("responseBody");
            report.append("Response: ");
            if (responseBody != null && !responseBody.toString().isEmpty()) {
              String responseStr = responseBody.toString();
              // Format JSON if possible
              if (responseStr.trim().startsWith("{") || responseStr.trim().startsWith("[")) {
                try {
                  responseStr = objectMapper.writerWithDefaultPrettyPrinter()
                      .writeValueAsString(objectMapper.readTree(responseStr));
                } catch (Exception e) {
                  // Use original if formatting fails
                }
              }
              report.append(responseStr.replace("\n", "\n  ")).append("\n");
            } else {
              int httpStatus = (int) statusCode;
              if (httpStatus >= 400) {
                report.append("[Error: HTTP ").append(httpStatus)
                    .append(" - No response body received]\n");
              } else {
                report.append("[No response body captured]\n");
              }
            }
            report.append("\n");
          }
          
          // Generate curl command
          Object method = event.data.get("method");
          Object url = event.data.get("url");
          if (method != null && url != null) {
            report.append("curl Command:\n");
            report.append("  curl -X ").append(method).append(" ").append(url);
            if (requestBody != null && !requestBody.toString().isEmpty()) {
              // Escape single quotes in the body for shell safety
              String bodyStr = requestBody.toString().replace("'", "'\\''");
              // Format as multi-line with proper line continuations
              report.append(" \\\n");
              report.append("    -H \"Content-Type: application/json\" \\\n");
              report.append("    -d '").append(bodyStr).append("'\n");
            } else {
              report.append("\n");
            }
            report.append("\n");
          }
        }
        
        // Process API call errors (timeouts, connection errors, etc.)
        for (ExecutionEvent event : apiErrorEvents) {
          report.append("API Call ").append(callNumber++).append(" [ERROR]\n");
          report.append("----------------------------------------\n\n");
          report.append("Method:  ").append(event.data.get("method")).append("\n");
          report.append("URL:     ").append(event.data.get("url")).append("\n\n");
          
          Object statusCode = event.data.get("statusCode");
          if (statusCode != null) {
            int httpStatus = ((Number) statusCode).intValue();
            if (httpStatus > 0) {
              report.append("Status:  ").append(httpStatus).append(" [FAILED]\n\n");
            }
          }
          
          Object errorType = event.data.get("errorType");
          Object errorMessage = event.data.get("errorMessage");
          if (errorType != null || errorMessage != null) {
            report.append("Error Type:    ").append(errorType != null ? errorType : "Unknown").append("\n");
            report.append("Error Message: ").append(errorMessage != null ? errorMessage : "No details available").append("\n\n");
          }
          
          Object durationMs = event.data.get("durationMs");
          if (durationMs != null) {
            report.append("Duration: ").append(durationMs).append("ms\n\n");
          }
          
          Object requestHeaders = event.data.get("requestHeaders");
          if (requestHeaders != null && requestHeaders instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, String> headers = (Map<String, String>) requestHeaders;
            if (!headers.isEmpty()) {
              report.append("Headers:\n");
              headers.forEach((k, v) -> report.append("  ").append(k).append(": ").append(v).append("\n"));
              report.append("\n");
            }
          }
          
          Object requestBody = event.data.get("requestBody");
          if (requestBody != null && !requestBody.toString().isEmpty()) {
            report.append("Request Body:\n");
            String bodyStr = requestBody.toString();
            // Format JSON if possible
            if (bodyStr.trim().startsWith("{")) {
              try {
                bodyStr = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(objectMapper.readTree(bodyStr));
              } catch (Exception e) {
                // Use original if formatting fails
              }
            }
            report.append("  ").append(bodyStr.replace("\n", "\n  ")).append("\n\n");
          }
          
          report.append("Response: [Error occurred - ").append(errorType != null ? errorType : "Unknown error")
              .append(": ").append(errorMessage != null ? errorMessage : "No details").append("]\n\n");
          
          // Generate curl command
          Object method = event.data.get("method");
          Object url = event.data.get("url");
          if (method != null && url != null) {
            report.append("curl Command:\n");
            report.append("  curl -X ").append(method).append(" ").append(url);
            if (requestBody != null && !requestBody.toString().isEmpty()) {
              // Escape single quotes in the body for shell safety
              String bodyStr = requestBody.toString().replace("'", "'\\''");
              // Format as multi-line with proper line continuations
              report.append(" \\\n");
              report.append("    -H \"Content-Type: application/json\" \\\n");
              report.append("    -d '").append(bodyStr).append("'\n");
            } else {
              report.append("\n");
            }
            report.append("\n");
          }
        }
        
        report.append("--------------------------------------------------------------------------------\n\n");
      }
      
      // API REQUEST PAYLOADS (duplicate of request bodies from API calls)
      if (!apiEvents.isEmpty()) {
        report.append("--------------------------------------------------------------------------------\n");
        report.append("API REQUEST PAYLOADS\n");
        report.append("--------------------------------------------------------------------------------\n\n");
        for (int i = 0; i < apiEvents.size(); i++) {
          ExecutionEvent event = apiEvents.get(i);
          Object requestBody = event.data.get("requestBody");
          if (requestBody != null && !requestBody.toString().isEmpty()) {
            report.append("Request ").append(i + 1);
            Object url = event.data.get("url");
            if (url != null) {
              // Extract service name from URL if possible
              String urlStr = url.toString();
              if (urlStr.contains("/")) {
                String[] parts = urlStr.split("/");
                if (parts.length > 2) {
                  String serviceName = parts[parts.length - 2];
                  report.append(" (").append(serviceName).append(")");
                }
              }
            }
            report.append("\n");
            report.append("----------------------------------------\n\n");
            String bodyStr = requestBody.toString();
            // Format JSON if possible
            if (bodyStr.trim().startsWith("{")) {
              try {
                bodyStr = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(objectMapper.readTree(bodyStr));
              } catch (Exception e) {
                // Use original if formatting fails
              }
            }
            report.append("  ").append(bodyStr.replace("\n", "\n  ")).append("\n\n");
          }
        }
        report.append("--------------------------------------------------------------------------------\n\n");
      }
      
      // EXECUTION TIMELINE
      report.append("--------------------------------------------------------------------------------\n");
      report.append("EXECUTION TIMELINE\n");
      report.append("--------------------------------------------------------------------------------\n\n");
      report.append("  Time       | Event                     | Details\n");
      report.append("  -----------+---------------------------+---------------------------------------\n");
      
      long startTime = 0;
      if (startEvent != null) {
        try {
          startTime = Instant.parse(startEvent.timestamp).toEpochMilli();
        } catch (Exception e) {
          // Ignore
        }
      }
      
      for (ExecutionEvent event : events) {
        String eventType = event.type;
        long eventTime = 0;
        try {
          eventTime = Instant.parse(event.timestamp).toEpochMilli();
        } catch (Exception e) {
          // Ignore
        }
        long relativeTime = startTime > 0 ? eventTime - startTime : 0;
        String timeStr = relativeTime + "ms";
        
        switch (eventType) {
          case "execution_started":
            Object userQuery = event.data.get("userQuery");
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | Execution started         | Query: ")
                .append(userQuery != null ? userQuery.toString() : "").append("\n");
            break;
          case "phase_change":
            Object phase = event.data.get("phase");
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | Phase change               | Phase: ").append(phase).append("\n");
            break;
          case "llm_inference_start":
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | LLM inference started      |\n");
            break;
          case "llm_inference_complete":
            Object duration = event.data.get("durationMs");
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | LLM inference completed    | Duration: ").append(duration).append("ms\n");
            break;
          case "tool_call":
            Object toolName = event.data.get("toolName");
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | Tool call                  | Tool: ").append(toolName).append("\n");
            break;
          case "code_generation":
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | Code generated             |\n");
            break;
          case "api_call":
            Object method = event.data.get("method");
            Object url = event.data.get("url");
            Object apiDuration = event.data.get("durationMs");
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | API call                   | ").append(method).append(" ")
                .append(url).append(" (").append(apiDuration).append("ms)\n");
            break;
          case "execution_complete":
            report.append("  ").append(String.format("%-10s", timeStr))
                .append(" | Execution completed        |\n");
            break;
        }
      }
      report.append("\n");
      
      // PERFORMANCE METRICS
      report.append("--------------------------------------------------------------------------------\n");
      report.append("PERFORMANCE METRICS\n");
      report.append("--------------------------------------------------------------------------------\n\n");
      report.append("  Total Duration:            ").append(totalDurationMs).append("ms (")
          .append(String.format("%.2f", totalDurationMs / 1000.0)).append("s)\n");
      report.append("  API Calls:                 ").append(apiEvents.size() + apiErrorEvents.size()).append("\n");
      if (totalApiTime > 0) {
        report.append("  Total API Time:             ").append(totalApiTime).append("ms\n");
      }
      if (totalLlmTime > 0) {
        report.append("  Total LLM Time:             ").append(totalLlmTime).append("ms\n");
      }
      report.append("\n");
      
      // ERRORS
      List<ExecutionEvent> errorEvents = apiEvents.stream()
          .filter(e -> {
            Object statusCode = e.data.get("statusCode");
            if (statusCode != null) {
              int httpStatus = (int) statusCode;
              return httpStatus >= 400;
            }
            return false;
          })
          .toList();
      
      // Combine HTTP errors and API call errors
      List<ExecutionEvent> allErrors = new ArrayList<>(errorEvents);
      allErrors.addAll(apiErrorEvents);
      
      if (!allErrors.isEmpty()) {
        report.append("--------------------------------------------------------------------------------\n");
        report.append("ERRORS\n");
        report.append("--------------------------------------------------------------------------------\n\n");
        int errorNum = 1;
        for (ExecutionEvent event : errorEvents) {
          report.append("Error ").append(errorNum++).append(":\n");
          report.append("----------------------------------------\n");
          Object statusCode = event.data.get("statusCode");
          Object url = event.data.get("url");
          Object method = event.data.get("method");
          report.append("  [ERROR] API call failed: ").append(method).append(" ")
              .append(url).append(" - HTTP ").append(statusCode).append("\n\n");
        }
        for (ExecutionEvent event : apiErrorEvents) {
          report.append("Error ").append(errorNum++).append(":\n");
          report.append("----------------------------------------\n");
          Object errorType = event.data.get("errorType");
          Object errorMessage = event.data.get("errorMessage");
          Object url = event.data.get("url");
          Object method = event.data.get("method");
          Object statusCode = event.data.get("statusCode");
          report.append("  [ERROR] ").append(errorType != null ? errorType : "Unknown Error")
              .append(": ").append(method).append(" ").append(url);
          if (statusCode != null && ((Number) statusCode).intValue() > 0) {
            report.append(" - HTTP ").append(statusCode);
          }
          report.append("\n");
          if (errorMessage != null && !errorMessage.toString().isEmpty()) {
            report.append("  Details: ").append(errorMessage).append("\n");
          }
          report.append("\n");
        }
      }
      
      // GENERATED CODE (at the bottom, complete)
      if (!codeEvents.isEmpty()) {
        report.append("--------------------------------------------------------------------------------\n");
        report.append("GENERATED CODE\n");
        report.append("--------------------------------------------------------------------------------\n\n");
        // If there's only one code block, print it directly without numbering
        if (codeEvents.size() == 1) {
          ExecutionEvent event = codeEvents.get(0);
          Object code = event.data.get("code");
          if (code != null && !code.toString().isEmpty()) {
            // Output complete code without truncation
            report.append(code.toString()).append("\n\n");
          }
        } else {
          // Multiple code blocks - show with numbering
          for (int i = 0; i < codeEvents.size(); i++) {
            ExecutionEvent event = codeEvents.get(i);
            Object code = event.data.get("code");
            if (code != null && !code.toString().isEmpty()) {
              report.append("Code Block ").append(i + 1).append("\n");
              report.append("----------------------------------------\n\n");
              // Output complete code without truncation
              report.append("  ").append(code.toString().replace("\n", "\n  ")).append("\n\n");
            }
          }
        }
        report.append("--------------------------------------------------------------------------------\n\n");
      }
      
      report.append("================================================================================\n");
      
      // Write report to file (using pre-determined path from startExecution)
      Path reportFile = executionReportPaths.get(executionId);
      if (reportFile == null && currentReportPath != null && 
          currentExecutionId != null && currentExecutionId.equals(executionId)) {
        // Fallback to currentReportPath if stored path not found but currentExecutionId matches
        reportFile = currentReportPath;
      }
      if (reportFile == null) {
        // Last resort: generate a new path
        String fileTimestamp = Instant.now().toString().replace(":", "-");
        reportFile = reportsDir.resolve("execution-" + fileTimestamp + ".txt");
        log.warn("Report path not found for executionId={}, generating new path: {}", executionId, reportFile);
      }
      
      // Ensure parent directory exists
      Files.createDirectories(reportFile.getParent());
      
      // Write report to file (only file I/O - no locks needed)
      Files.write(reportFile, report.toString().getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
      
      log.info("Execution report generated: {}", reportFile);
      System.out.println("Execution report: " + reportFile);
      
      // Verify file was actually written
      if (!Files.exists(reportFile)) {
        log.error("Report file was not created: {}", reportFile);
        return null;
      }
      
      long fileSize = Files.size(reportFile);
      if (fileSize == 0) {
        log.warn("Report file is empty: {}", reportFile);
      } else {
        log.debug("Report file created successfully: {} ({} bytes)", reportFile, fileSize);
      }
      
      return reportFile.toString();
      
    } catch (Exception e) {
      log.error("Failed to generate execution report for executionId={}", executionId, e);
      return null;
    }
  }

  /**
   * Execution event data structure.
   */
  public static class ExecutionEvent {
    public String type;
    public String executionId;
    public String timestamp;
    public Map<String, Object> data;

    public ExecutionEvent() {
      // Default constructor for Jackson
    }

    public ExecutionEvent(String type, String executionId, Map<String, Object> data) {
      this.type = type;
      this.executionId = executionId;
      this.timestamp = Instant.now().toString();
      this.data = data;
    }
  }
}

