package com.gentoro.onemcp.cache;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for normalizing natural-language prompts into S-SQL statements (cache_spec v10.0).
 *
 * <p>Uses LLM to convert user prompts into a single S-SQL statement according to the
 * S-SQL restricted SQL dialect. The normalizer validates against a conceptual schema
 * to ensure only canonical tables and columns are used.
 *
 * <p>Produces a Prompt Schema (PS) containing: { table, ssql, columns, cache_key }
 */
public class SsqlNormalizer {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(SsqlNormalizer.class);

  private final OneMcp oneMcp;

  public SsqlNormalizer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Normalize a natural-language prompt into S-SQL and produce Prompt Schema (PS).
   *
   * @param prompt the user's natural-language prompt
   * @param schema the schema to validate against
   * @return Prompt Schema containing { table, ssql, values, columns, cache_key }
   * @throws ExecutionException if normalization fails after all retries
   */
  public PromptSchema normalize(String prompt, ConceptualSchema schema) throws ExecutionException {
    NormalizationResult result = normalizeWithRetry(prompt, schema, 3); // Max 3 attempts
    
    // Extract table and columns from JSON IR or S-SQL (may be empty if normalization failed)
    String table = null;
    List<String> columns = new ArrayList<>();
    String canonicalSsql = null;
    SsqlJsonIr jsonIr = result.jsonIr;
    
    // Initialize canonicalizer for cache key generation
    SsqlCanonicalizer canonicalizer = new SsqlCanonicalizer();
    
    if (jsonIr != null) {
      // Use JSON IR
      table = jsonIr.getTable();
      columns = jsonIr.getColumns();
      
      // Render canonical S-SQL from JSON IR
      SsqlJsonIrRenderer renderer = new SsqlJsonIrRenderer();
      try {
        canonicalSsql = renderer.render(jsonIr);
      } catch (Exception e) {
        log.error("Failed to render S-SQL from JSON IR", e);
        throw new ExecutionException("Failed to render S-SQL from JSON IR: " + e.getMessage(), e);
      }
    } else if (result.ssql != null && !result.ssql.trim().isEmpty()) {
      // Fallback: use direct S-SQL (backward compatibility)
      table = extractTableName(result.ssql);
      columns = extractColumns(result.ssql, schema, table);
      
      // Canonicalize existing S-SQL
      canonicalSsql = canonicalizer.canonicalize(result.ssql);
    } else {
      // No SQL - use prompt as basis for cache key (normalized)
      canonicalSsql = "FAILED:" + prompt.trim().toLowerCase().replaceAll("\\s+", " ");
    }
    
    // Generate cache key from canonical S-SQL
    String cacheKey = canonicalizer.generateCacheKey(canonicalSsql);
    
    return new PromptSchema(table, canonicalSsql, result.values, columns, cacheKey, result.note, jsonIr);
  }

  /**
   * Normalize with retry logic and error feedback.
   */
  private NormalizationResult normalizeWithRetry(String prompt, ConceptualSchema schema, int maxAttempts) 
      throws ExecutionException {
    int attempt = 0;
    List<String> previousErrors = new ArrayList<>();

    while (attempt < maxAttempts) {
      attempt++;
      
      try {
        NormalizationResult result = normalizeOnce(prompt, schema, attempt > 1 ? previousErrors : null);
        
        // Validate result (JSON IR preferred, S-SQL fallback)
        List<String> validationErrors = new ArrayList<>();
        if (result.jsonIr != null) {
          // Validate JSON IR
          try {
            result.jsonIr.validate();
            // Validate table exists in schema
            if (!schema.hasTable(result.jsonIr.getTable())) {
              validationErrors.add("JSON IR references table '" + result.jsonIr.getTable() + "' which is not in schema");
            }
          } catch (IllegalArgumentException e) {
            validationErrors.add("JSON IR validation failed: " + e.getMessage());
          }
        } else if (result.ssql != null && !result.ssql.trim().isEmpty()) {
          // Validate S-SQL (fallback)
          validationErrors = validateSsql(result.ssql, schema);
        } else {
          // No JSON IR and no S-SQL - only valid if there's a note explaining why
          if (result.note == null || result.note.trim().isEmpty()) {
            validationErrors.add("No JSON IR or S-SQL provided and no explanation note");
          }
        }
        
        if (validationErrors.isEmpty()) {
          if (result.jsonIr != null) {
            log.info("Successfully normalized prompt to JSON IR (attempt {}): operation={}, table={}", 
                attempt, result.jsonIr.getOperation(), result.jsonIr.getTable());
          } else {
            log.info("Successfully normalized prompt to S-SQL (attempt {}): {}", attempt, result.ssql);
          }
          return result;
        }
        
        // Validation failed - collect errors for retry
        String errorMsg = String.join("; ", validationErrors);
        log.warn("S-SQL validation failed (attempt {}): {}", attempt, errorMsg);
        previousErrors = validationErrors;
        
        // Log error to inference logger so it appears in report
        // We need to log with non-zero tokens so it doesn't get filtered out
        // Use a minimal token count to ensure it appears in the report
        if (oneMcp.inferenceLogger() != null) {
          String errorResponse = "[VALIDATION ERROR - RETRYING]\n" +
              "Errors: " + errorMsg + "\n" +
              "Rejected S-SQL: " + result.ssql + "\n" +
              "The S-SQL will be regenerated with error feedback.";
          oneMcp.inferenceLogger().logLlmInferenceComplete(
              "normalize", 1, 1, 2, // Use minimal token counts so it appears in report
              errorResponse, false);
        }
        
        if (attempt < maxAttempts) {
          log.info("Retrying SQL normalization (attempt {}/{}) with error feedback", 
              attempt + 1, maxAttempts);
          continue;
        } else {
          throw new ExecutionException(
              "SQL normalization failed after " + maxAttempts + " attempts. " +
              "Last errors: " + String.join("; ", validationErrors));
        }
      } catch (ExecutionException e) {
        // If it's a validation error, retry with feedback
        if (attempt < maxAttempts && e.getMessage() != null && 
            (e.getMessage().contains("validation") || e.getMessage().contains("Invalid"))) {
          previousErrors.add(e.getMessage());
          log.info("Retrying SQL normalization (attempt {}/{}) after error: {}", 
              attempt + 1, maxAttempts, e.getMessage());
          continue;
        }
        // Otherwise, rethrow
        throw e;
      }
    }
    
    throw new ExecutionException("SQL normalization failed after " + maxAttempts + " attempts");
  }

  /**
   * Single normalization attempt.
   */
  private NormalizationResult normalizeOnce(String prompt, ConceptualSchema schema, List<String> previousErrors) 
      throws ExecutionException {
    if (prompt == null || prompt.trim().isEmpty()) {
      throw new IllegalArgumentException("Prompt cannot be null or empty");
    }
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null");
    }

    log.debug("Normalizing prompt to S-SQL: {}", prompt);

    // Validate schema has tables
    if (schema.getTables() == null || schema.getTables().isEmpty()) {
      throw new ExecutionException("Schema has no tables");
    }

    // Prepare prompt context
    Map<String, Object> context = new HashMap<>();
    context.put("user_prompt", prompt.trim());
    // Serialize schema to JSON string for better LLM consumption
    String schemaJson = serializeSchemaToJson(schema);
    context.put("schema", schemaJson);
    
    // Add error feedback if retrying - this will be added to messages below
    String errorFeedback = null;
    if (previousErrors != null && !previousErrors.isEmpty()) {
      StringBuilder feedback = new StringBuilder();
      feedback.append("Your previous SQL was REJECTED due to validation errors:\n\n");
      for (String error : previousErrors) {
        feedback.append("❌ ").append(error).append("\n");
      }
      feedback.append("\nPlease correct your SQL according to the instructions above and try again.");
      errorFeedback = feedback.toString();
    }

    log.debug("Schema summary - Tables: {}", schema.getTables().size());

    // Add error_feedback to context (even if null, so template can check for it)
    context.put("error_feedback", errorFeedback != null ? errorFeedback : "");

    // Load and render S-SQL normalization prompt
    PromptRepository promptRepo = oneMcp.promptRepository();
    PromptTemplate template = promptRepo.get("ssql-normalize");
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("ssql-normalize", context);

    List<LlmClient.Message> messages;
    try {
      messages = session.renderMessages();
    } catch (Exception e) {
      log.error("Failed to render S-SQL normalization prompt template: {}", e.getMessage(), e);
      throw new ExecutionException(
          "Failed to render S-SQL normalization prompt template: " + e.getMessage(), e);
    }
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from S-SQL normalization prompt");
    }

    // Call LLM
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    log.debug("Calling LLM for SQL normalization");
    
    // Create telemetry sink to set phase name for logging
    final Map<String, Object> sinkAttributes = new HashMap<>();
    sinkAttributes.put("phase", "normalize");
    sinkAttributes.put("cacheHit", false);
    
    LlmClient.TelemetrySink tokenSink =
        new LlmClient.TelemetrySink() {
          @Override
          public void startChild(String name) {}
          
          @Override
          public void endCurrentOk(java.util.Map<String, Object> attrs) {}
          
          @Override
          public void endCurrentError(java.util.Map<String, Object> attrs) {}
          
          @Override
          public void addUsage(Long promptTokens, Long completionTokens, Long totalTokens) {
            // Token usage is tracked by LLM client automatically
          }
          
          @Override
          public java.util.Map<String, Object> currentAttributes() {
            return sinkAttributes;
          }
        };
    
    // Get temperature from template (defaults to 0.0 for deterministic normalization)
    Float temperature = template.temperature().orElse(0.0f);
    
    String response;
    try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
      response = llmClient.chat(messages, java.util.Collections.emptyList(), false, null, temperature);
    } catch (Exception e) {
      log.error("LLM call failed for SQL normalization", e);
      throw new ExecutionException("LLM call failed for SQL normalization: " + e.getMessage(), e);
    }

    // Extract JSON from response (should contain ssql, values, columns, etc.)
    String jsonStr = extractJsonFromResponse(response);
    if (jsonStr == null || jsonStr.trim().isEmpty()) {
      // Fallback: try to extract SQL directly
      String sql = extractSqlFromResponse(response);
      if (sql == null || sql.trim().isEmpty()) {
        log.error("No content found in LLM response");
        log.debug("LLM response was: {}", response);
        throw new ExecutionException("No content found in LLM response");
      }
      // Extract values from SQL and replace with placeholders
      return extractValuesAndPlaceholders(sql);
    }

    // Parse JSON response
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = JacksonUtility.getJsonMapper().readValue(jsonStr, Map.class);
      
      // Try to parse JSON IR first (preferred)
      Object jsonIrObj = resultMap.get("json_ir");
      SsqlJsonIr jsonIr = null;
      if (jsonIrObj != null) {
        try {
          String jsonIrStr = JacksonUtility.getJsonMapper().writeValueAsString(jsonIrObj);
          jsonIr = JacksonUtility.getJsonMapper().readValue(jsonIrStr, SsqlJsonIr.class);
          jsonIr.validate();
          log.debug("Parsed JSON IR from LLM response");
        } catch (Exception e) {
          log.warn("Failed to parse JSON IR, falling back to S-SQL: {}", e.getMessage());
        }
      }
      
      // Fallback: try to parse direct S-SQL (backward compatibility)
      String ssql = (String) resultMap.get("ssql");
      Object valuesObj = resultMap.get("values");
      String note = (String) resultMap.get("note"); // Optional explanation/caveat
      
      // If neither JSON IR nor S-SQL is present, check if there's a note explaining why
      if (jsonIr == null && (ssql == null || ssql.trim().isEmpty())) {
        if (note != null && !note.trim().isEmpty()) {
          // LLM provided explanation for why it couldn't generate SQL
          log.info("Normalization failed with explanation: {}", note);
          return new NormalizationResult(null, null, new HashMap<>(), note.trim());
        } else {
          // Try to extract explanation from the raw response
          String explanation = extractExplanationFromResponse(response);
          if (explanation != null && !explanation.trim().isEmpty()) {
            log.info("Normalization failed, extracted explanation from response");
            return new NormalizationResult(null, null, new HashMap<>(), explanation.trim());
          }
          throw new ExecutionException("LLM response missing both 'json_ir' and 'ssql' fields and no explanation provided");
        }
      }
      
      // Handle both Map (preferred) and List (backward compatibility) formats for values
      Map<String, Object> valuesMap = new HashMap<>();
      if (valuesObj != null) {
        if (valuesObj instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> map = (Map<String, Object>) valuesObj;
          valuesMap.putAll(map);
        } else if (valuesObj instanceof List) {
          // Backward compatibility: convert list to map with generic keys
          @SuppressWarnings("unchecked")
          List<Object> list = (List<Object>) valuesObj;
          for (int i = 0; i < list.size(); i++) {
            valuesMap.put("value_" + i, list.get(i));
          }
        }
      }
      
      if (jsonIr != null) {
        return new NormalizationResult(jsonIr, valuesMap, note != null ? note.trim() : null);
      } else {
        // Fallback to direct S-SQL
        return new NormalizationResult(ssql.trim(), null, valuesMap, note != null ? note.trim() : null);
      }
    } catch (Exception e) {
      log.warn("Failed to parse JSON response, trying direct SQL extraction: {}", e.getMessage());
      String sql = extractSqlFromResponse(response);
      if (sql == null || sql.trim().isEmpty()) {
        throw new ExecutionException("Failed to parse LLM response: " + e.getMessage());
      }
      return extractValuesAndPlaceholders(sql);
    }
  }

  /**
   * Extract values from SQL and replace with placeholders.
   * This is a fallback method that generates generic keys.
   */
  private NormalizationResult extractValuesAndPlaceholders(String sql) {
    Map<String, Object> values = new HashMap<>();
    String ssql = sql;
    int valueIndex = 0;
    
    // Simple extraction: find string literals, numbers, and replace with ?
    // This is a simplified version - a full SQL parser would be better
    java.util.regex.Pattern stringPattern = java.util.regex.Pattern.compile("'([^']*)'");
    java.util.regex.Matcher matcher = stringPattern.matcher(ssql);
    while (matcher.find()) {
      String value = matcher.group(1);
      // Try to infer a meaningful key from context
      String key = inferKeyFromContext(ssql, matcher.start(), valueIndex, value);
      values.put(key, value);
      valueIndex++;
    }
    ssql = matcher.replaceAll("?");
    
    // Extract numbers
    java.util.regex.Pattern numberPattern = java.util.regex.Pattern.compile("\\b(\\d+)\\b");
    matcher = numberPattern.matcher(ssql);
    while (matcher.find()) {
      String numStr = matcher.group(1);
      Object numValue;
      try {
        numValue = Integer.parseInt(numStr);
      } catch (NumberFormatException e) {
        numValue = Long.parseLong(numStr);
      }
      // Try to infer a meaningful key from context
      String key = inferKeyFromContext(ssql, matcher.start(), valueIndex, numStr);
      values.put(key, numValue);
      valueIndex++;
    }
    ssql = numberPattern.matcher(ssql).replaceAll("?");
    
    return new NormalizationResult(ssql.trim(), values);
  }
  
  /**
   * Infer a meaningful key name from SQL context.
   * This is a heuristic - the LLM should provide better keys.
   */
  private String inferKeyFromContext(String sql, int position, int index, Object value) {
    // Look at surrounding context to infer key name
    String before = sql.substring(Math.max(0, position - 50), position).toLowerCase();
    String after = sql.substring(position, Math.min(sql.length(), position + 50)).toLowerCase();
    String context = before + " " + after;
    
    // Check for common patterns
    if (context.contains("date") || context.contains("year") || context.contains("time")) {
      if (context.contains("like")) {
        return "date_filter";
      } else if (context.contains("year")) {
        return "year";
      } else {
        return "date_value";
      }
    } else if (context.contains("amount") || context.contains("price") || context.contains("cost")) {
      if (context.contains(">") || context.contains(">=")) {
        return "min_amount";
      } else if (context.contains("<") || context.contains("<=")) {
        return "max_amount";
      } else {
        return "amount";
      }
    } else if (context.contains("limit")) {
      return "limit";
    } else if (context.contains("offset")) {
      return "offset";
    } else if (context.contains("id")) {
      return "id";
    } else if (context.contains("name")) {
      return "name_filter";
    } else if (context.contains("status")) {
      return "status";
    } else if (context.contains("category")) {
      return "category";
    }
    
    // Fallback to generic key
    return "value_" + index;
  }

  /**
   * Extract explanation from LLM response when normalization fails.
   * Looks for text explanations in Optional[...] wrappers or plain text.
   */
  private String extractExplanationFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String text = response.trim();
    
    // Remove Optional[...] wrapper if present
    if (text.startsWith("Optional[") || text.startsWith("optional[")) {
      int start = text.indexOf('[') + 1;
      int end = text.lastIndexOf(']');
      if (end > start) {
        text = text.substring(start, end).trim();
      }
    }
    
    // Remove markdown code blocks
    if (text.startsWith("```")) {
      int firstNewline = text.indexOf('\n');
      if (firstNewline > 0) {
        text = text.substring(firstNewline + 1);
      }
      if (text.endsWith("```")) {
        text = text.substring(0, text.length() - 3).trim();
      }
    }
    
    // If it looks like an explanation (contains explanation keywords but no SQL keywords)
    String upper = text.toUpperCase();
    boolean hasExplanationKeywords = upper.contains("CANNOT") || upper.contains("CANNOT") || 
        upper.contains("IMPOSSIBLE") || upper.contains("CONSTRAINT") || 
        upper.contains("REQUIRE") || upper.contains("NOT ALLOWED") ||
        upper.contains("DOES NOT ALLOW");
    boolean hasSqlKeywords = upper.contains("SELECT") || upper.contains("FROM") || 
        upper.contains("WHERE") || upper.contains("INSERT") || upper.contains("UPDATE");
    
    if (hasExplanationKeywords && !hasSqlKeywords && text.length() > 50) {
      // Clean up the text - remove excessive whitespace
      text = text.replaceAll("\\s+", " ").trim();
      return text;
    }
    
    return null;
  }

  /**
   * Extract JSON from LLM response.
   */
  private String extractJsonFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String jsonStr = response.trim();
    if (jsonStr.startsWith("```json")) {
      jsonStr = jsonStr.substring(7).trim();
    } else if (jsonStr.startsWith("```")) {
      jsonStr = jsonStr.substring(3).trim();
    }

    if (jsonStr.endsWith("```")) {
      jsonStr = jsonStr.substring(0, jsonStr.length() - 3).trim();
    }

    int firstBrace = jsonStr.indexOf('{');
    int lastBrace = jsonStr.lastIndexOf('}');

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      jsonStr = jsonStr.substring(firstBrace, lastBrace + 1);
    }

    return jsonStr.trim();
  }

  /**
   * Validate S-SQL and return list of errors (empty if valid).
   */
  private List<String> validateSsql(String ssql, ConceptualSchema schema) {
    List<String> errors = new ArrayList<>();
    
    // Check for Optional wrapper
    if (ssql.contains("Optional[") || ssql.contains("optional[")) {
      errors.add("S-SQL must not be wrapped in Optional[...]. Output the S-SQL statement directly.");
    }
    
    // Check for STRFTIME or other date extraction functions
    String upperSsql = ssql.toUpperCase();
    if (upperSsql.contains("STRFTIME") || upperSsql.contains("EXTRACT") || 
        upperSsql.contains("DATE_PART") || upperSsql.contains("YEAR(") ||
        upperSsql.contains("MONTH(") || upperSsql.contains("DAY(")) {
      errors.add("DO NOT use STRFTIME, EXTRACT, or date extraction functions. " +
                 "Use direct column comparisons with parameterized queries (e.g., column_yyyy = ? or column_datetime = ?). " +
                 "Prefer = ? or IN (?) over LIKE when possible.");
    }
    
    // Validate against conceptual schema
    try {
      validateSsqlAgainstSchema(ssql, schema);
    } catch (ExecutionException e) {
      errors.add(e.getMessage());
    }
    
    return errors;
  }

  /**
   * Serialize schema to JSON string for LLM consumption.
   */
  private String serializeSchemaToJson(ConceptualSchema schema) {
    try {
      return JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(schema);
    } catch (Exception e) {
      log.error("Failed to serialize schema to JSON", e);
      return "{}";
    }
  }

  /**
   * Determine if tool-calling should be used instead of full schema.
   * 
   * <p>Tool-calling is more efficient when the full schema is large because:
   * - Full schema: single call with all data
   * - Tool-calling: two calls, but only fetches columns for chosen table
   * 
   * <p>Break-even point: when full_schema_bytes > 2 × (prompt_bytes + table_catalog_bytes) + avg_columns_bytes
   * 
   * <p>Since we're comparing similar JSON structures, bytes are a good proxy for tokens.
   * 
   * @param fullSchemaJson the full schema serialized as JSON
   * @param prompt the user prompt
   * @return true if tool-calling should be used, false if full schema is better
   */
  private boolean shouldUseToolCalling(String fullSchemaJson, String prompt) {
    if (fullSchemaJson == null) {
      return false;
    }
    
    int fullSchemaBytes = fullSchemaJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    int promptBytes = prompt != null ? prompt.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : 0;
    
    // Estimate tool-calling overhead:
    // - Table catalog: just table names (much smaller than full schema)
    // - Tool definitions: ~200-300 bytes
    // - The prompt + table_catalog gets sent twice in tool-calling
    // - Average columns per table: estimate from full schema size / num tables
    
    // Rough heuristic: if full schema > 5KB, tool-calling is likely better
    // This accounts for the overhead of sending prompt + table_catalog twice
    // (typically ~1-2KB) plus tool definitions (~300 bytes)
    int thresholdBytes = 5 * 1024; // 5KB threshold
    
    if (fullSchemaBytes < thresholdBytes) {
      log.debug("Using full schema ({} bytes < {} bytes threshold)", fullSchemaBytes, thresholdBytes);
      return false;
    }
    
    // For larger schemas, estimate if tool-calling would be better
    // Estimate: 2 × (prompt + table_catalog) + tool_defs + avg_columns
    // Table catalog is roughly: full_schema / (1 + avg_columns_per_table_ratio)
    // For simplicity, assume table catalog is ~20% of full schema (just table names)
    int estimatedTableCatalogBytes = fullSchemaBytes / 5;
    int toolDefinitionsBytes = 300; // Rough estimate
    int estimatedAvgColumnsBytes = fullSchemaBytes / 10; // Rough estimate for one table's columns
    
    int toolCallingOverheadBytes = 2 * (promptBytes + estimatedTableCatalogBytes) + 
                                   toolDefinitionsBytes + estimatedAvgColumnsBytes;
    
    boolean useToolCalling = fullSchemaBytes > toolCallingOverheadBytes;
    log.debug("Schema size: {} bytes, estimated tool-calling overhead: {} bytes, using: {}", 
        fullSchemaBytes, toolCallingOverheadBytes, useToolCalling ? "tool-calling" : "full schema");
    
    return useToolCalling;
  }

  /**
   * Extract columns used in S-SQL statement.
   */
  private List<String> extractColumns(String ssql, ConceptualSchema schema, String table) {
    List<String> columns = new ArrayList<>();
    if (table == null || !schema.hasTable(table)) {
      return columns;
    }
    
    // Simple extraction - get all columns from schema that appear in S-SQL
    List<String> tableColumns = schema.getColumns(table);
    String lowerSsql = ssql.toLowerCase();
    for (String col : tableColumns) {
      if (lowerSsql.contains(col.toLowerCase())) {
        columns.add(col);
      }
    }
    
    return columns;
  }

  /**
   * Extract SQL content from LLM response, handling markdown code blocks and Optional wrapper.
   */
  private String extractSqlFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String sql = response.trim();

    // Remove Optional[...] wrapper if present
    if (sql.startsWith("Optional[") || sql.startsWith("optional[")) {
      sql = sql.substring(sql.indexOf('[') + 1);
      // Find matching closing bracket
      int lastBracket = sql.lastIndexOf(']');
      if (lastBracket >= 0) {
        sql = sql.substring(0, lastBracket).trim();
      }
    }

    // Remove markdown code block markers
    if (sql.startsWith("```sql")) {
      sql = sql.substring(6).trim();
    } else if (sql.startsWith("```")) {
      sql = sql.substring(3).trim();
    }

    if (sql.endsWith("```")) {
      sql = sql.substring(0, sql.length() - 3).trim();
    }

    // Try to find SQL statement boundaries
    int firstSemicolon = sql.indexOf(';');
    if (firstSemicolon >= 0) {
      sql = sql.substring(0, firstSemicolon).trim();
    }

    return sql.trim();
  }

  /**
   * Validate that S-SQL uses only tables and columns from the schema.
   */
  private void validateSsqlAgainstSchema(String ssql, ConceptualSchema schema)
      throws ExecutionException {
    String upperSsql = ssql.toUpperCase();
    
    // Extract table name from FROM clause
    int fromIndex = upperSsql.indexOf(" FROM ");
    if (fromIndex < 0) {
      fromIndex = upperSsql.indexOf("UPDATE ");
      if (fromIndex < 0) {
        fromIndex = upperSsql.indexOf("INSERT INTO ");
        if (fromIndex < 0) {
          fromIndex = upperSsql.indexOf("DELETE FROM ");
        }
      }
    }
    
    if (fromIndex < 0) {
      throw new ExecutionException("S-SQL statement must contain FROM, UPDATE, INSERT INTO, or DELETE FROM");
    }

    // Extract table name (simplified - full SQL parser would be better)
    String tableName = extractTableName(ssql, fromIndex);
    if (tableName == null || !schema.hasTable(tableName)) {
      throw new ExecutionException(
          "S-SQL references table '" + tableName + "' which is not in schema");
    }

    // TODO: Validate columns are in schema (would require full SQL parsing)
    log.debug("S-SQL validation passed for table: {}", tableName);
  }

  /**
   * Result of normalization containing JSON IR (preferred) or S-SQL (fallback) and extracted values.
   */
  private static class NormalizationResult {
    final String ssql; // Fallback: direct S-SQL (backward compatibility)
    final SsqlJsonIr jsonIr; // Preferred: JSON IR
    final Map<String, Object> values;
    final String note; // Optional explanation/caveat

    NormalizationResult(String ssql, Map<String, Object> values) {
      this(ssql, null, values, null);
    }
    
    NormalizationResult(String ssql, Map<String, Object> values, String note) {
      this(ssql, null, values, note);
    }
    
    NormalizationResult(SsqlJsonIr jsonIr, Map<String, Object> values, String note) {
      this(null, jsonIr, values, note);
    }
    
    NormalizationResult(String ssql, SsqlJsonIr jsonIr, Map<String, Object> values, String note) {
      this.ssql = ssql != null ? ssql : "";
      this.jsonIr = jsonIr;
      this.values = values != null ? values : new HashMap<>();
      this.note = note;
    }
    
    // Constructor for backward compatibility with List (fallback extraction)
    NormalizationResult(String ssql, List<Object> valuesList) {
      this(ssql, null, valuesList, null);
    }
    
    NormalizationResult(String ssql, List<Object> valuesList, String note) {
      this(ssql, null, valuesList, note);
    }
    
    NormalizationResult(String ssql, SsqlJsonIr jsonIr, List<Object> valuesList, String note) {
      this.ssql = ssql != null ? ssql : "";
      this.jsonIr = jsonIr;
      this.values = new HashMap<>();
      // Convert list to map with generic keys (fallback)
      if (valuesList != null) {
        for (int i = 0; i < valuesList.size(); i++) {
          this.values.put("value_" + i, valuesList.get(i));
        }
      }
      this.note = note;
    }
  }

  /**
   * Extract table name from S-SQL statement (simplified).
   */
  private String extractTableName(String ssql, int startIndex) {
    String upperSsql = ssql.toUpperCase();
    String lowerSsql = ssql.toLowerCase();
    
    // Find the keyword
    int keywordEnd = -1;
    if (upperSsql.substring(startIndex).startsWith(" FROM ")) {
      keywordEnd = startIndex + 6;
    } else if (upperSsql.substring(startIndex).startsWith("UPDATE ")) {
      keywordEnd = startIndex + 7;
    } else if (upperSsql.substring(startIndex).startsWith("INSERT INTO ")) {
      keywordEnd = startIndex + 12;
    } else if (upperSsql.substring(startIndex).startsWith("DELETE FROM ")) {
      keywordEnd = startIndex + 12;
    }
    
    if (keywordEnd < 0) {
      return null;
    }
    
    // Extract table name (until whitespace or special character)
    int end = keywordEnd;
    while (end < ssql.length() && 
           (Character.isLetterOrDigit(ssql.charAt(end)) || ssql.charAt(end) == '_')) {
      end++;
    }
    
    return lowerSsql.substring(keywordEnd, end).trim();
  }
  
  /**
   * Extract table name from S-SQL statement (public method for PromptSchema generation).
   */
  private String extractTableName(String ssql) {
    String upperSsql = ssql.toUpperCase();
    
    int fromIndex = upperSsql.indexOf(" FROM ");
    if (fromIndex >= 0) {
      return extractTableName(ssql, fromIndex);
    }
    
    int updateIndex = upperSsql.indexOf("UPDATE ");
    if (updateIndex >= 0) {
      return extractTableName(ssql, updateIndex);
    }
    
    int insertIndex = upperSsql.indexOf("INSERT INTO ");
    if (insertIndex >= 0) {
      return extractTableName(ssql, insertIndex);
    }
    
    int deleteIndex = upperSsql.indexOf("DELETE FROM ");
    if (deleteIndex >= 0) {
      return extractTableName(ssql, deleteIndex);
    }
    
    return null;
  }
}

