package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Prompt Schema (PS) - JSON structure without SQL.
 * 
 * <p>Structured, SQL-inspired JSON representation with expression-tree WHERE.
 * No SQL strings are generated anywhere in the system.
 * 
 * <p>Structure:
 * {
 *   "operation": "select | insert | update | delete",
 *   "table": "string",
 *   "fields": ["col1", { "agg": "sum", "column": "amount" }, ...],
 *   "where": <ExpressionTree or null>,
 *   "group_by": ["col1"],
 *   "order_by": ["col1"],
 *   "limit": 10,
 *   "offset": 0,
 *   "values": { "v1": "literal1", "v2": 10, "v3": "abc" },
 *   "columns": ["col1", "amount", ...],
 *   "cache_key": "md5...",
 *   "original_prompt": "user's original prompt"
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public class PromptSchema {
  @JsonProperty("operation")
  private String operation; // "select" | "insert" | "update" | "delete"

  @JsonProperty("table")
  private String table;

  @JsonProperty("fields")
  private List<Object> fields; // String or Map with "agg" and "column"

  @JsonProperty("where")
  private ExpressionTree where; // Expression tree or null

  @JsonProperty("group_by")
  private List<String> groupBy;

  @JsonProperty("order_by")
  private List<String> orderBy;

  @JsonProperty("limit")
  private Integer limit;

  @JsonProperty("offset")
  private Integer offset;

  @JsonProperty("values")
  private Map<String, Object> values; // Dictionary of labeled values: { "v1": <literal>, "v2": <literal> }

  @JsonProperty("columns")
  private List<String> columns; // All columns referenced

  @JsonProperty("cache_key")
  private String cacheKey;

  @JsonProperty("current_time")
  private String currentTime;

  @JsonProperty("note")
  private String note; // Optional explanation/caveat

  @JsonProperty("original_prompt")
  private String originalPrompt; // Original user prompt

  public PromptSchema() {
    this.currentTime = Instant.now().toString();
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation != null ? operation.toLowerCase() : null;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<Object> getFields() {
    return fields;
  }

  public void setFields(List<Object> fields) {
    this.fields = fields;
  }

  public ExpressionTree getWhere() {
    return where;
  }

  public void setWhere(ExpressionTree where) {
    this.where = where;
  }

  public List<String> getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(List<String> groupBy) {
    this.groupBy = groupBy;
  }

  public List<String> getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(List<String> orderBy) {
    this.orderBy = orderBy;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
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

  public String getOriginalPrompt() {
    return originalPrompt;
  }

  public void setOriginalPrompt(String originalPrompt) {
    this.originalPrompt = originalPrompt;
  }

  /**
   * Set shape object and flatten it to flat fields for backward compatibility.
   * This allows parsing both the new shape format and the old flat format.
   * 
   * @param shape the shape object containing group_by, aggregates, order_by, limit, offset
   */
  @JsonSetter("shape")
  @SuppressWarnings("unchecked")
  public void setShape(Map<String, Object> shape) {
    if (shape == null) {
      return;
    }
    
    // Extract group_by from shape
    if (shape.containsKey("group_by")) {
      Object groupByObj = shape.get("group_by");
      if (groupByObj instanceof List) {
        this.groupBy = new ArrayList<>();
        for (Object item : (List<?>) groupByObj) {
          if (item instanceof String) {
            this.groupBy.add((String) item);
          }
        }
      }
    }
    
    // Extract aggregates from shape and convert to fields format
    if (shape.containsKey("aggregates")) {
      Object aggregatesObj = shape.get("aggregates");
      if (aggregatesObj instanceof List) {
        this.fields = new ArrayList<>();
        for (Object aggObj : (List<?>) aggregatesObj) {
          if (aggObj instanceof Map) {
            Map<String, Object> agg = (Map<String, Object>) aggObj;
            Map<String, Object> fieldObj = new HashMap<>();
            if (agg.containsKey("field")) {
              fieldObj.put("column", agg.get("field"));
            }
            if (agg.containsKey("function")) {
              fieldObj.put("agg", agg.get("function"));
            }
            this.fields.add(fieldObj);
          }
        }
      }
    }
    
    // Extract order_by from shape
    if (shape.containsKey("order_by")) {
      Object orderByObj = shape.get("order_by");
      if (orderByObj instanceof List) {
        this.orderBy = new ArrayList<>();
        for (Object item : (List<?>) orderByObj) {
          if (item instanceof String) {
            this.orderBy.add((String) item);
          }
        }
      }
    }
    
    // Extract limit from shape (convert string to integer if needed)
    if (shape.containsKey("limit")) {
      Object limitObj = shape.get("limit");
      if (limitObj == null) {
        this.limit = null;
      } else if (limitObj instanceof Integer) {
        this.limit = (Integer) limitObj;
      } else if (limitObj instanceof String) {
        try {
          this.limit = Integer.parseInt((String) limitObj);
        } catch (NumberFormatException e) {
          this.limit = null;
        }
      } else if (limitObj instanceof Number) {
        this.limit = ((Number) limitObj).intValue();
      }
    }
    
    // Extract offset from shape (convert string to integer if needed)
    if (shape.containsKey("offset")) {
      Object offsetObj = shape.get("offset");
      if (offsetObj == null) {
        this.offset = null;
      } else if (offsetObj instanceof Integer) {
        this.offset = (Integer) offsetObj;
      } else if (offsetObj instanceof String) {
        try {
          this.offset = Integer.parseInt((String) offsetObj);
        } catch (NumberFormatException e) {
          this.offset = null;
        }
      } else if (offsetObj instanceof Number) {
        this.offset = ((Number) offsetObj).intValue();
      }
    }
  }
}

