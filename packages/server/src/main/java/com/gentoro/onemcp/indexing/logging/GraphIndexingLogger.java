package com.gentoro.onemcp.indexing.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.indexing.GraphIndexingTypes;
import com.gentoro.onemcp.indexing.graph.GraphEdge;
import com.gentoro.onemcp.indexing.graph.nodes.*;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Utility class for logging indexing operations and LLM interactions.
 *
 * <p>This class handles all file-based logging for the graph indexing service, separating logging
 * concerns from the main indexing logic for cleaner code organization.
 */
public class GraphIndexingLogger {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(GraphIndexingLogger.class);

  private final String handbookName;

  /**
   * Create a new graph indexing logger.
   *
   * @param handbookName the name of the handbook being indexed
   */
  public GraphIndexingLogger(String handbookName) {
    this.handbookName = handbookName;
  }

  /**
   * Log LLM prompt messages to a file.
   *
   * <p>Creates a separate file for each prompt using a timestamp in the filename.
   *
   * @param serviceSlug the service slug
   * @param messages the messages sent to LLM
   */
  public void logLLMPrompt(String serviceSlug, List<LlmClient.Message> messages) {
    String filename =
        String.format(
            "%s-llm-prompt-%s-%s.log",
            sanitizedHandbookName(), getTimestamp(), sanitizeForFilename(serviceSlug));

    writeLogFile(
        filename,
        serviceSlug,
        writer -> {
          writer.write("PROMPT MESSAGES:\n");
          writer.write("-".repeat(80) + "\n");

          for (int i = 0; i < messages.size(); i++) {
            LlmClient.Message msg = messages.get(i);
            writer.write(
                String.format(
                    "\n[Message %d/%d - Role: %s]\n", i + 1, messages.size(), msg.role()));
            writer.write("-".repeat(80) + "\n");
            writer.write(msg.content());
            writer.write("\n");
          }
        });
  }

  /**
   * Log complete graph structure to a file.
   *
   * <p>Creates a separate file for each graph using a timestamp in the filename. Logs all entities,
   * operations, examples, and relationships in JSON format.
   *
   * @param serviceSlug the service slug
   * @param result the graph extraction result containing all nodes and edges
   */
  public void logCompleteGraph(
      String serviceSlug, GraphIndexingTypes.GraphExtractionResult result) {
    String filename =
        String.format(
            "%s-llm-graph-%s-%s.log",
            sanitizedHandbookName(), getTimestamp(), sanitizeForFilename(serviceSlug));

    writeLogFile(
        filename,
        serviceSlug,
        writer -> {
          writer.write("COMPLETE GRAPH STRUCTURE:\n");
          writer.write("-".repeat(80) + "\n");

          // Build complete graph structure as JSON
          Map<String, Object> graphStructure = new java.util.HashMap<>();
          graphStructure.put("serviceSlug", serviceSlug);
          graphStructure.put(
              "timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

          // Convert entities to maps
          List<Map<String, Object>> entitiesList = new java.util.ArrayList<>();
          for (EntityNode entity : result.entities()) {
            entitiesList.add(entity.toMap());
          }
          graphStructure.put("entities", entitiesList);

          // Convert fields to maps
          List<Map<String, Object>> fieldsList = new java.util.ArrayList<>();
          for (FieldNode field : result.fields()) {
            fieldsList.add(field.toMap());
          }
          graphStructure.put("fields", fieldsList);

          // Convert operations to maps
          List<Map<String, Object>> operationsList = new java.util.ArrayList<>();
          for (OperationNode operation : result.operations()) {
            operationsList.add(operation.toMap());
          }
          graphStructure.put("operations", operationsList);

          // Convert examples to maps
          List<Map<String, Object>> examplesList = new java.util.ArrayList<>();
          for (ExampleNode example : result.examples()) {
            examplesList.add(example.toMap());
          }
          graphStructure.put("examples", examplesList);

          // Convert documentations to maps
          List<Map<String, Object>> documentationsList = new java.util.ArrayList<>();
          for (DocumentationNode documentation : result.documentations()) {
            documentationsList.add(documentation.toMap());
          }
          graphStructure.put("documentations", documentationsList);

          // Convert relationships to maps
          List<Map<String, Object>> relationshipsList = new java.util.ArrayList<>();
          for (GraphEdge edge : result.relationships()) {
            Map<String, Object> edgeMap = edge.toMap();
            edgeMap.put("fromKey", edge.getFromKey());
            edgeMap.put("toKey", edge.getToKey());
            relationshipsList.add(edgeMap);
          }
          graphStructure.put("relationships", relationshipsList);

          // Add summary statistics
          Map<String, Object> summary = new java.util.HashMap<>();
          summary.put("totalEntities", result.entities().size());
          summary.put("totalFields", result.fields().size());
          summary.put("totalOperations", result.operations().size());
          summary.put("totalExamples", result.examples().size());
          summary.put("totalDocumentations", result.documentations().size());
          summary.put("totalRelationships", result.relationships().size());
          graphStructure.put("summary", summary);

          // Write as formatted JSON
          ObjectMapper mapper = JacksonUtility.getJsonMapper();
          String jsonOutput =
              mapper.writerWithDefaultPrettyPrinter().writeValueAsString(graphStructure);
          writer.write(jsonOutput);
          writer.write("\n");
        });
  }

  /**
   * Log LLM response to a file.
   *
   * <p>Creates a separate file for each response using a timestamp in the filename.
   *
   * @param serviceSlug the service slug
   * @param response the response from LLM
   */
  public void logLLMResponse(String serviceSlug, String response) {
    String filename =
        String.format(
            "%s-llm-response-%s-%s.log",
            sanitizedHandbookName(), getTimestamp(), sanitizeForFilename(serviceSlug));

    writeLogFile(
        filename,
        serviceSlug,
        writer -> {
          writer.write("LLM RESPONSE:\n");
          writer.write("-".repeat(80) + "\n");
          writer.write(response);
          writer.write("\n");
        });
  }

  /**
   * Log JSON parsing errors to a separate file for debugging.
   *
   * @param serviceSlug the service slug
   * @param llmResponse the original LLM response
   * @param error the parsing error
   */
  public void logLLMParseError(String serviceSlug, String llmResponse, Exception error) {
    String filename =
        String.format(
            "%s-llm-parse-error-%s-%s.log",
            sanitizedHandbookName(), getTimestamp(), sanitizeForFilename(serviceSlug));

    writeLogFile(
        filename,
        serviceSlug,
        writer -> {
          writer.write("JSON PARSING ERROR\n");
          writer.write("=".repeat(80) + "\n");
          writer.write(String.format("ERROR TYPE: %s\n", error.getClass().getSimpleName()));
          writer.write(String.format("ERROR MESSAGE: %s\n", error.getMessage()));
          writer.write("-".repeat(80) + "\n");
          writer.write("ORIGINAL LLM RESPONSE:\n");
          writer.write("-".repeat(80) + "\n");
          writer.write(llmResponse);
          writer.write("\n");
          writer.write("-".repeat(80) + "\n");
          writer.write("STACK TRACE:\n");
          writer.write("-".repeat(80) + "\n");

          java.io.PrintWriter pw = new java.io.PrintWriter(writer);
          error.printStackTrace(pw);
          pw.flush();
          writer.write("\n");
        });
  }

  /**
   * Write content to a log file with standard header and footer.
   *
   * @param filename the log filename
   * @param serviceSlug the service slug for header
   * @param contentWriter lambda to write the actual content
   */
  private void writeLogFile(String filename, String serviceSlug, LogContentWriter contentWriter) {
    try {
      File logsDir = new File("logs");
      if (!logsDir.exists()) {
        logsDir.mkdirs();
      }

      File logFile = new File(logsDir, filename);

      try (FileWriter writer = new FileWriter(logFile, false)) {
        writer.write("=".repeat(80) + "\n");
        writer.write(
            String.format(
                "TIMESTAMP: %s\n",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
        writer.write(String.format("SERVICE: %s\n", serviceSlug));
        writer.write("=".repeat(80) + "\n");

        contentWriter.write(writer);

        writer.write("=".repeat(80) + "\n");
        writer.flush();
      }

      log.debug("Logged to file: {}", logFile.getAbsolutePath());
    } catch (Exception e) {
      log.warn("Failed to write log file: {}", filename, e);
    }
  }

  /**
   * Get a timestamp string for log filenames.
   *
   * @return timestamp in format yyyyMMdd-HHmmss-SSS
   */
  private String getTimestamp() {
    return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"));
  }

  /**
   * Sanitize a string for use in filenames.
   *
   * @param input the input string
   * @return sanitized string safe for filenames
   */
  private String sanitizeForFilename(String input) {
    if (input == null) return "unknown";
    return input.replaceAll("[^a-zA-Z0-9_-]", "_");
  }

  private String sanitizedHandbookName() {
    String name = handbookName;
    if (name == null || name.isBlank()) {
      name = "unknown_handbook";
    }
    return name.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase();
  }

  /**
   * Functional interface for writing custom content to log files.
   *
   * <p>Used by {@link #writeLogFile(String, String, LogContentWriter)} to allow different types of
   * log content to be written with consistent formatting.
   */
  @FunctionalInterface
  private interface LogContentWriter {
    /**
     * Write content to the provided file writer.
     *
     * @param writer the file writer to write to
     * @throws Exception if writing fails
     */
    void write(FileWriter writer) throws Exception;
  }
}
