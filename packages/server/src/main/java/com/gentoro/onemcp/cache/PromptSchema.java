package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Prompt Schema (PS) for S-SQL normalization.
 *
 * <p>The Normalizer produces a Prompt Schema containing:
 * - table: the table name
 * - ssql: the S-SQL statement with ? placeholders (null if normalization failed)
 * - values: extracted literal values as key/value pairs (e.g., {"date_filter": "2024%", "min_amount": 1000})
 * - columns: list of columns used
 * - cache_key: hash of canonical S-SQL (or prompt if normalization failed)
 * - current_time: ISO-8601 timestamp when schema was created
 * - note: optional explanation/caveat (explanation if ssql is null, caveat if ssql exists)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptSchema {
  @JsonProperty("table")
  private String table;

  @JsonProperty("ssql")
  private String ssql;

  @JsonProperty("values")
  private Map<String, Object> values;

  @JsonProperty("columns")
  private List<String> columns;

  @JsonProperty("cache_key")
  private String cacheKey;

  @JsonProperty("current_time")
  private String currentTime;

  @JsonProperty("note")
  private String note;

  @JsonProperty("json_ir")
  private SsqlJsonIr jsonIr;

  public PromptSchema() {}

  public PromptSchema(String table, String ssql, Map<String, Object> values, List<String> columns, String cacheKey) {
    this(table, ssql, values, columns, cacheKey, null, null);
  }

  public PromptSchema(String table, String ssql, Map<String, Object> values, List<String> columns, String cacheKey, String note) {
    this(table, ssql, values, columns, cacheKey, note, null);
  }

  public PromptSchema(String table, String ssql, Map<String, Object> values, List<String> columns, String cacheKey, String note, SsqlJsonIr jsonIr) {
    this.table = table;
    this.ssql = ssql;
    this.values = values;
    this.columns = columns;
    this.cacheKey = cacheKey;
    this.currentTime = Instant.now().toString();
    this.note = note;
    this.jsonIr = jsonIr;
  }
  
  // Constructor for list-based values (backward compatibility)
  public PromptSchema(String table, String ssql, List<Object> valuesList, List<String> columns, String cacheKey) {
    this.table = table;
    this.ssql = ssql;
    this.values = new java.util.HashMap<>();
    if (valuesList != null) {
      for (int i = 0; i < valuesList.size(); i++) {
        this.values.put("value_" + i, valuesList.get(i));
      }
    }
    this.columns = columns;
    this.cacheKey = cacheKey;
    this.currentTime = Instant.now().toString();
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getSsql() {
    return ssql;
  }

  public void setSsql(String ssql) {
    this.ssql = ssql;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  public void setValues(Map<String, Object> values) {
    this.values = values;
  }

  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public String getCacheKey() {
    return cacheKey;
  }

  public void setCacheKey(String cacheKey) {
    this.cacheKey = cacheKey;
  }

  public String getCurrentTime() {
    return currentTime;
  }

  public void setCurrentTime(String currentTime) {
    this.currentTime = currentTime;
  }

  public String getNote() {
    return note;
  }

  public void setNote(String note) {
    this.note = note;
  }

  public SsqlJsonIr getJsonIr() {
    return jsonIr;
  }

  public void setJsonIr(SsqlJsonIr jsonIr) {
    this.jsonIr = jsonIr;
  }
}
