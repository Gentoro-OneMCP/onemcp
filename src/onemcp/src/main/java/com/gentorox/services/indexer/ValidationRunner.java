package com.gentorox.services.indexer;

import com.gentorox.services.validation.FoundationValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.nio.file.Files;

public final class ValidationRunner {
  private static final Logger log = LoggerFactory.getLogger(ValidationRunner.class);
  private ValidationRunner() {}
  
  public static void run() {
    String foundation = System.getenv().getOrDefault("FOUNDATION_DIR", "/var/foundation");
    
    log.info("🚀 Starting foundation validation...");
    log.info("📁 Foundation directory: {}", foundation);
    log.info("");
    
    // Use the comprehensive validation service
    FoundationValidationService validationService = new FoundationValidationService();
    FoundationValidationService.ValidationResult result = validationService.validateFoundation(foundation);
    
    // Log all validation results with enhanced formatting
    result.logResults(log);
    
    // Log additional telemetry configuration checks
    log.info("");
    log.info("🔧 Additional Configuration Checks:");
    logTelemetryConfiguration();
    
    // Log MCP server information
    log.info("");
    log.info("🌐 MCP server expected at /mcp (http), server.port controls port (default 8080).");
    
    // Print final summary
    log.info("");
    log.info(result.getSummaryMessage());
    
    // Exit with error code if validation failed
    if (!result.isValid()) {
      log.error("");
      log.error("💡 To fix validation errors:");
      log.error("   1. Ensure Agent.md exists and contains proper agent instructions");
      log.error("   2. Add at least one .md file in the docs/ directory");
      log.error("   3. Add valid OpenAPI specification in the apis/ directory");
      log.error("   4. Check the detailed error messages above for specific issues");
      System.exit(1);
    } else {
      log.info("");
      log.info("🎯 Your foundation is ready! You can now run the agent with:");
      log.info("   docker run -v $(pwd):/var/foundation -p 8080:8080 admingentoro/gentoro:latest");
    }
  }
  
  private static void logTelemetryConfiguration() {
    String otlp = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    File localOtelCfg = new File("/etc/otel-collector-config.yaml");
    if (otlp != null && !otlp.isBlank()) {
      log.info("✓ OTLP endpoint set: {}", otlp);
    } else if (localOtelCfg.exists() && readable(localOtelCfg)) {
      log.info("✓ Local OTEL collector config present at {}", localOtelCfg.getAbsolutePath());
    } else {
      log.warn("No OTLP endpoint or local collector config detected; telemetry will default to local logging.");
    }
  }
  
  private static boolean readable(File f) { 
    try { 
      return Files.isReadable(f.toPath()); 
    } catch (Exception e) { 
      return false; 
    } 
  }
  
  /**
   * Main method for standalone execution
   */
  public static void main(String[] args) {
    run();
  }
}
