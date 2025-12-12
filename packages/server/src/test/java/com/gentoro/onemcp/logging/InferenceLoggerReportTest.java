package com.gentoro.onemcp.logging;

import com.gentoro.onemcp.OneMcp;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

public class InferenceLoggerReportTest {

  @TempDir
  Path tempDir;

  private OneMcp oneMcp;
  private InferenceLogger logger;
  private Path logFile;
  private Path reportsDir;

  @BeforeEach
  void setUp() throws IOException {
    // Create a temporary log directory
    Path logDir = tempDir.resolve("logs");
    Files.createDirectories(logDir);
    
    // Create a temporary reports directory
    reportsDir = tempDir.resolve("reports");
    Files.createDirectories(reportsDir);
    
    // Create log file
    logFile = logDir.resolve("onemcp.log");
    Files.createFile(logFile);
    
    // Set system properties to point to our temp directories
    System.setProperty("user.home", tempDir.toString());
    System.setProperty("ONEMCP_HOME_DIR", tempDir.toString());
    System.setProperty("ONEMCP_LOG_DIR", logDir.toString());
    
    // Create a minimal OneMcp instance - we'll use reflection to bypass initialization
    String[] appArgs = new String[] {
        "--mode", "server"
    };
    oneMcp = new OneMcp(appArgs);
    
    // Create logger using reflection to bypass the constructor's detectReportMode call
    // which requires OneMcp to be initialized
    try {
      // Create logger instance
      logger = new InferenceLogger(oneMcp);
      
      // Use reflection to force report mode enabled and set reports directory
      java.lang.reflect.Field reportModeField = InferenceLogger.class.getDeclaredField("reportModeEnabled");
      reportModeField.setAccessible(true);
      reportModeField.setBoolean(logger, true);
      
      java.lang.reflect.Field reportsDirField = InferenceLogger.class.getDeclaredField("reportsDirectory");
      reportsDirField.setAccessible(true);
      reportsDirField.set(logger, reportsDir);
    } catch (Exception e) {
      // If reflection fails, try to initialize OneMcp (might fail if port is in use)
      try {
        oneMcp.initialize();
        logger = new InferenceLogger(oneMcp);
      } catch (Exception e2) {
        throw new RuntimeException("Failed to create InferenceLogger: " + e.getMessage(), e);
      }
    }
  }

  @Test
  void testServerLogCapturedInReport() throws IOException {
    // Simulate production environment: CLI pipes stdout/stderr to app.log
    // But Logback might also write to onemcp.log
    // The code should find app.log (the larger/active file)
    
    Path logDir = logFile.getParent();
    Path appLogFile = logDir.resolve("app.log");
    Path onemcpLogFile = logDir.resolve("onemcp.log");
    
    // Create onemcp.log with some old content (simulating Logback output)
    try (FileWriter writer = new FileWriter(onemcpLogFile.toFile(), true)) {
      writer.write("2025-12-10 09:00:00.000 [main] INFO  Old Logback entry\n");
      writer.write("2025-12-10 09:00:01.000 [main] INFO  Another old entry\n");
    }
    
    // Write substantial initial content to app.log (simulating CLI piping stdout/stderr)
    // In production, app.log accumulates a lot of output, making it larger than onemcp.log
    // This is the file that will have new entries during execution
    try (FileWriter writer = new FileWriter(appLogFile.toFile(), true)) {
      // Write enough content to make app.log larger than onemcp.log
      for (int i = 0; i < 100; i++) {
        writer.write(String.format("2025-12-10 09:00:%02d.000 [main] INFO  Test - Initial log entry %d\n", i, i));
      }
      writer.write("2025-12-10 10:00:00.000 [main] INFO  Test - Initial log entry (before execution)\n");
    }
    
    // Verify app.log is larger (like production: app.log is 2.9MB, onemcp.log is 2.1KB)
    long appLogSize = Files.size(appLogFile);
    long onemcpLogSize = Files.size(onemcpLogFile);
    assertTrue(appLogSize > onemcpLogSize, 
        String.format("app.log should be larger than onemcp.log to simulate production (app.log=%d, onemcp.log=%d)", 
            appLogSize, onemcpLogSize));
    
    // Start execution
    String executionId = UUID.randomUUID().toString();
    String reportPath = logger.startExecution(executionId, "test query");
    assertNotNull(reportPath, "Report path should be returned");
    
    // Write log entries during execution to app.log (simulating CLI piping stdout/stderr)
    try (FileWriter writer = new FileWriter(appLogFile.toFile(), true)) {
      writer.write("2025-12-10 10:00:01.000 [thread-1] INFO  Test - Log during execution 1\n");
      writer.write("2025-12-10 10:00:02.000 [thread-1] ERROR Test - Error during execution\n");
      writer.write("2025-12-10 10:00:03.000 [thread-1] INFO  Test - Log during execution 2\n");
    }
    
    // Also write something to onemcp.log to simulate Logback (but it should be ignored)
    try (FileWriter writer = new FileWriter(onemcpLogFile.toFile(), true)) {
      writer.write("2025-12-10 10:00:01.500 [logback] INFO  This should NOT appear in report\n");
    }
    
    // Log some events
    logger.logLlmInferenceStart("normalize");
    logger.logLlmInferenceComplete("normalize", 1000L, 100L, 50L, "test response");
    
    // Complete execution and generate report
    long startTime = System.currentTimeMillis() - 5000; // 5 seconds ago
    String finalReportPath = logger.completeExecution(executionId, 5000L, true, startTime);
    assertNotNull(finalReportPath, "Final report path should be returned");
    
    // Read the report
    Path reportFile = Paths.get(finalReportPath);
    assertTrue(Files.exists(reportFile), "Report file should exist");
    
    String reportContent = Files.readString(reportFile);
    assertNotNull(reportContent, "Report content should not be null");
    
    // Verify server log section exists
    assertTrue(reportContent.contains("SERVER LOG"), 
        "Report should contain SERVER LOG section");
    
    // Verify the log entries written during execution to app.log are in the report
    assertTrue(reportContent.contains("Log during execution 1"), 
        "Report should contain log entry written during execution");
    assertTrue(reportContent.contains("Error during execution"), 
        "Report should contain error log entry");
    assertTrue(reportContent.contains("Log during execution 2"), 
        "Report should contain second log entry");
    
    // Verify initial log entry is NOT in the report (it was written before execution started)
    assertFalse(reportContent.contains("Initial log entry"), 
        "Report should NOT contain log entry written before execution started");
    
    // Verify that onemcp.log entries are NOT in the report (we should read from app.log, not onemcp.log)
    assertFalse(reportContent.contains("This should NOT appear in report"), 
        "Report should NOT contain entries from onemcp.log (should read from app.log instead)");
    
    // Verify that old onemcp.log entries are NOT in the report
    assertFalse(reportContent.contains("Old Logback entry"), 
        "Report should NOT contain old entries from onemcp.log");
    
    // Print the SERVER LOG section for verification
    int serverLogStart = reportContent.indexOf("SERVER LOG");
    if (serverLogStart >= 0) {
      int serverLogEnd = reportContent.indexOf("╔══════════════════════════════════════════════════════════════════════════════╗", serverLogStart);
      if (serverLogEnd < 0) {
        serverLogEnd = reportContent.length();
      }
      String serverLogSection = reportContent.substring(serverLogStart, serverLogEnd);
      System.out.println("\n=== SERVER LOG SECTION FROM REPORT ===");
      System.out.println(serverLogSection);
      System.out.println("=== END SERVER LOG SECTION ===\n");
    }
  }

  @Test
  void testServerLogEmptyWhenNoNewEntries() throws IOException {
    // Simulate production: create both app.log and onemcp.log
    Path logDir = logFile.getParent();
    Path appLogFile = logDir.resolve("app.log");
    
    // Write some initial log content to app.log
    try (FileWriter writer = new FileWriter(appLogFile.toFile(), true)) {
      writer.write("2025-12-10 10:00:00.000 [main] INFO  Test - Initial log entry\n");
    }
    
    // Start execution
    String executionId = UUID.randomUUID().toString();
    logger.startExecution(executionId, "test query");
    
    // Don't write any new log entries during execution
    
    // Complete execution
    long startTime = System.currentTimeMillis() - 1000;
    String finalReportPath = logger.completeExecution(executionId, 1000L, true, startTime);
    
    // Read the report
    String reportContent = Files.readString(Paths.get(finalReportPath));
    
    // Verify server log section exists but shows no entries
    assertTrue(reportContent.contains("SERVER LOG"), 
        "Report should contain SERVER LOG section");
    assertTrue(reportContent.contains("No server log entries captured") || 
               !reportContent.contains("Initial log entry"),
        "Report should indicate no new log entries or not show old entries");
  }
}

