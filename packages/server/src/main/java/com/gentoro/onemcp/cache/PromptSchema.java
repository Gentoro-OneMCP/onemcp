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
 * Prompt Schema (PS) - Canonical conceptual representation of one intent.
 * 
 * <p>According to cache spec v21, PS structure:
 * {
 *   "action": "search" | "update" | "create" | "delete",
 *   "entity": "sale" | "customer" | "product" | ...,
 *   "filter": [...],
 *   "params": { "v1": "2024", ... },
 *   "shape": {
 *     "group_by": [...],
 *     "aggregates": [...],
 *     "order_by": [...],
 *     "limit": "10",
 *     "offset": "0"
 *   }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public class PromptSchema {
  // Spec v21 fields
  @JsonProperty("action")
  private String action; // "search" | "update" | "create" | "delete"

  @JsonProperty("entity")
  private String entity;

  @JsonProperty("filter")
  private List<Map<String, Object>> filter; // Array of filter conditions per spec

  @JsonProperty("params")
  private Map<String, Object> params; // Dictionary of parameter values per spec

  // Shape is the source of truth for group_by, order_by, limit, offset
  private Map<String, Object> shape; // Internal storage for shape

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

  // Getters/setters for action/entity (spec v21)
  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action != null ? action.toLowerCase() : null;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }

  // Getters/setters for filter and params (spec v21)
  public List<Map<String, Object>> getFilter() {
    return filter;
  }

  @JsonSetter("filter")
  @SuppressWarnings("unchecked")
  public void setFilter(List<Map<String, Object>> filter) {
    this.filter = filter;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  @JsonSetter("params")
  public void setParams(Map<String, Object> params) {
    this.params = params;
  }

  // Helper methods to access shape fields
  @com.fasterxml.jackson.annotation.JsonIgnore
  @SuppressWarnings("unchecked")
  public List<String> getGroupBy() {
    Map<String, Object> s = getShape();
    if (s != null && s.containsKey("group_by")) {
      Object gb = s.get("group_by");
      if (gb instanceof List) {
        return (List<String>) gb;
      }
    }
    return new ArrayList<>();
  }

  @JsonSetter("group_by")
  @SuppressWarnings("unchecked")
  public void setGroupBy(Object groupByObj) {
    // Set via shape
    Map<String, Object> s = getShape();
    if (s == null) {
      s = new HashMap<>();
    }
    if (groupByObj == null) {
      s.put("group_by", new ArrayList<>());
    } else if (groupByObj instanceof List) {
      List<String> groupBy = new ArrayList<>();
      for (Object item : (List<?>) groupByObj) {
        if (item instanceof String) {
          groupBy.add((String) item);
        } else if (item instanceof Map) {
          Map<String, Object> itemMap = (Map<String, Object>) item;
          if (itemMap.containsKey("column")) {
            Object columnObj = itemMap.get("column");
            if (columnObj instanceof String) {
              groupBy.add((String) columnObj);
            }
          }
        }
      }
      s.put("group_by", groupBy);
    } else {
      s.put("group_by", new ArrayList<>());
    }
    setShape(s);
  }

  @com.fasterxml.jackson.annotation.JsonIgnore
  @SuppressWarnings("unchecked")
  public List<String> getOrderBy() {
    Map<String, Object> s = getShape();
    if (s != null && s.containsKey("order_by")) {
      Object ob = s.get("order_by");
      if (ob instanceof List) {
        return (List<String>) ob;
      }
    }
    return new ArrayList<>();
  }

  @JsonSetter("order_by")
  @SuppressWarnings("unchecked")
  public void setOrderBy(Object orderByObj) {
    // Set via shape
    Map<String, Object> s = getShape();
    if (s == null) {
      s = new HashMap<>();
    }
    if (orderByObj == null) {
      s.put("order_by", new ArrayList<>());
    } else if (orderByObj instanceof List) {
      List<String> orderBy = new ArrayList<>();
      for (Object item : (List<?>) orderByObj) {
        if (item instanceof String) {
          orderBy.add((String) item);
        } else if (item instanceof Map) {
          Map<String, Object> itemMap = (Map<String, Object>) item;
          if (itemMap.containsKey("column")) {
            Object columnObj = itemMap.get("column");
            if (columnObj instanceof String) {
              orderBy.add((String) columnObj);
            }
          }
        }
      }
      s.put("order_by", orderBy);
    } else {
      s.put("order_by", new ArrayList<>());
    }
    setShape(s);
  }

  @com.fasterxml.jackson.annotation.JsonIgnore
  public Integer getLimit() {
    Map<String, Object> s = getShape();
    if (s != null && s.containsKey("limit")) {
      Object lim = s.get("limit");
      if (lim == null) {
        return null;
      } else if (lim instanceof Integer) {
        return (Integer) lim;
      } else if (lim instanceof String) {
        try {
          return Integer.parseInt((String) lim);
        } catch (NumberFormatException e) {
          return null;
        }
      } else if (lim instanceof Number) {
        return ((Number) lim).intValue();
      }
    }
    return null;
  }

  public void setLimit(Integer limit) {
    // Set via shape
    Map<String, Object> s = getShape();
    if (s == null) {
      s = new HashMap<>();
    }
    s.put("limit", limit != null ? limit.toString() : null);
    setShape(s);
  }

  @com.fasterxml.jackson.annotation.JsonIgnore
  public Integer getOffset() {
    Map<String, Object> s = getShape();
    if (s != null && s.containsKey("offset")) {
      Object off = s.get("offset");
      if (off == null) {
        return null;
      } else if (off instanceof Integer) {
        return (Integer) off;
      } else if (off instanceof String) {
        try {
          return Integer.parseInt((String) off);
        } catch (NumberFormatException e) {
          return null;
        }
      } else if (off instanceof Number) {
        return ((Number) off).intValue();
      }
    }
    return null;
  }

  public void setOffset(Integer offset) {
    // Set via shape
    Map<String, Object> s = getShape();
    if (s == null) {
      s = new HashMap<>();
    }
    s.put("offset", offset != null ? offset.toString() : null);
    setShape(s);
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
   * Get shape object (for serialization).
   * Returns shape in spec v21 format: { group_by, aggregates, order_by, limit, offset }
   */
  @JsonProperty("shape")
  public Map<String, Object> getShape() {
    if (shape == null) {
      // Initialize empty shape
      shape = new HashMap<>();
      shape.put("group_by", new ArrayList<>());
      shape.put("aggregates", new ArrayList<>());
      shape.put("order_by", new ArrayList<>());
      shape.put("limit", null);
      shape.put("offset", null);
    }
    return shape;
  }

  /**
   * Set shape object.
   * 
   * @param shapeValue the shape object containing group_by, aggregates, order_by, limit, offset
   */
  @JsonSetter("shape")
  @SuppressWarnings("unchecked")
  public void setShape(Map<String, Object> shapeValue) {
    if (shapeValue == null) {
      this.shape = null;
      return;
    }
    
    // Store shape directly
    this.shape = new HashMap<>(shapeValue);
  }
}
