package com.gentoro.onemcp.cache.dag;

import com.gentoro.onemcp.utility.JacksonUtility;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

/**
 * Executes custom logic code (TypeScript/JavaScript) for CustomLogic nodes.
 * 
 * <p>This is the escape hatch for API-specific transformations that cannot
 * be expressed using ConvertValue or MapEnum.
 */
public class CustomLogicExecutor {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(CustomLogicExecutor.class);

  /**
   * Execute custom logic code.
   * 
   * @param code the code to execute
   * @param language language ("ts" or "js")
   * @param args arguments to pass to the function
   * @return result of execution
   */
  public Object execute(String code, String language, Map<String, Object> args) {
    if (code == null || code.trim().isEmpty()) {
      throw new IllegalArgumentException("CustomLogic code cannot be empty");
    }

    // For now, delegate to the existing executeScript operation
    // This reuses the TypeScript execution infrastructure
    try {
      Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "onemcp-ts-temp");
      Files.createDirectories(tempDir);

      String uuid = UUID.randomUUID().toString();
      Path jsFile = tempDir.resolve("custom_" + uuid + ".js");
      Path resultFile = tempDir.resolve("custom_result_" + uuid + ".json");

      // Wrap code in a function that sets result
      String inputJson = JacksonUtility.getJsonMapper().writeValueAsString(args);
      String wrappedCode = String.format("""
          const input = %s;
          let result;
          try {
            %s
            // If code doesn't set result, try to extract it
            if (typeof result === 'undefined') {
              // Try to find a return statement or last expression
              result = input.value || input;
            }
          } catch (error) {
            throw new Error('CustomLogic execution failed: ' + error.message);
          }
          const fs = require('fs');
          fs.writeFileSync('%s', JSON.stringify({ result: result }));
          """, inputJson, code, resultFile.toString());

      Files.writeString(jsFile, wrappedCode);

      // Execute using Node.js
      ProcessBuilder pb = new ProcessBuilder("node", jsFile.toString());
      pb.directory(tempDir.toFile());
      Process process = pb.start();

      boolean finished = process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
      if (!finished) {
        process.destroyForcibly();
        throw new RuntimeException("CustomLogic execution timed out");
      }

      if (process.exitValue() != 0) {
        if (Files.exists(resultFile)) {
          String errorContent = Files.readString(resultFile);
          com.fasterxml.jackson.databind.JsonNode errorJson = 
              JacksonUtility.getJsonMapper().readTree(errorContent);
          if (errorJson.has("error")) {
            throw new RuntimeException("CustomLogic execution failed: " + 
                errorJson.get("error").asText());
          }
        }
        throw new RuntimeException("CustomLogic execution failed with exit code " + 
            process.exitValue());
      }

      // Read result
      if (!Files.exists(resultFile)) {
        throw new RuntimeException("CustomLogic did not produce result file");
      }

      String resultContent = Files.readString(resultFile);
      com.fasterxml.jackson.databind.JsonNode resultJson = 
          JacksonUtility.getJsonMapper().readTree(resultContent);

      if (resultJson.has("error")) {
        throw new RuntimeException("CustomLogic execution error: " + 
            resultJson.get("error").asText());
      }

      // Cleanup
      try {
        Files.deleteIfExists(jsFile);
        Files.deleteIfExists(resultFile);
      } catch (Exception e) {
        log.debug("Failed to cleanup custom logic files", e);
      }

      return JacksonUtility.getJsonMapper().convertValue(
          resultJson.get("result"), Object.class);
    } catch (Exception e) {
      log.error("Failed to execute CustomLogic: {}", e.getMessage(), e);
      throw new RuntimeException("CustomLogic execution failed: " + e.getMessage(), e);
    }
  }
}





