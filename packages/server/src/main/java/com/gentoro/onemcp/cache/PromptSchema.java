package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Prompt Schema (PS) - Canonical, deterministic representation of a natural-language prompt.
 *
 * <p>A Prompt Schema contains:
 *
 * <ul>
 *   <li>action: Canonical verb from dictionary.actions
 *   <li>entities: List of canonical nouns from dictionary.entities
 *   <li>group_by: Optional list of field names to group by (order is significant)
 *   <li>params: Map of field → parameter values (literals, operators, aggregates, etc.)
 * </ul>
 *
 * <p>The PSK (Prompt Schema Key) is generated from (action, sorted(entities),
 * sorted(params.keys()), group_by), where group_by is in declared order. Param values are excluded
 * for cache reuse.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptSchema {
  @JsonProperty("action")
  private String action;

  @JsonProperty("entities")
  private List<String> entities = new ArrayList<>();

  @JsonProperty("group_by")
  private List<String> groupBy = new ArrayList<>();

  @JsonProperty("params")
  private Map<String, Object> params = new HashMap<>();

  @JsonProperty("cache_key")
  private String cacheKey;

  public PromptSchema() {}

  public PromptSchema(String action, List<String> entities, Map<String, Object> params) {
    this.action = action;
    this.entities = entities != null ? new ArrayList<>(entities) : new ArrayList<>();
    this.params = params != null ? new HashMap<>(params) : new HashMap<>();
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public List<String> getEntities() {
    return entities;
  }

  public void setEntities(List<String> entities) {
    this.entities = entities != null ? new ArrayList<>(entities) : new ArrayList<>();
  }

  public List<String> getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(List<String> groupBy) {
    this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : new ArrayList<>();
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public void setParams(Map<String, Object> params) {
    this.params = params != null ? new HashMap<>(params) : new HashMap<>();
  }

  public String getCacheKey() {
    return cacheKey;
  }

  public void setCacheKey(String cacheKey) {
    this.cacheKey = cacheKey;
  }

  /**
   * Generate and set the cache key for this schema. The cache key is derived from (action,
   * sorted(entities), sorted(params.keys()), group_by), where group_by is in declared order (not
   * sorted). Param values are excluded for cache reuse across different parameter values.
   */
  public void generateCacheKey() {
    PromptSchemaKey psk = new PromptSchemaKey(this);
    this.cacheKey = psk.getStringKey(); // Use human-readable format for visibility
  }

  /**
   * Validate that cache key components are in the dictionary. Cache key is generated from: (action,
   * sorted(entities), sorted(params.keys()), group_by)
   *
   * @param dictionary the dictionary to validate against
   * @return list of validation errors (empty if valid)
   */
  public List<String> validate(PromptDictionary dictionary) {
    List<String> errors = new ArrayList<>();

    // Validate action (used in cache key)
    if (action == null || action.isEmpty()) {
      errors.add("Action is required for cache key");
    } else if (!"local".equals(action) && !dictionary.hasAction(action)) {
      errors.add("Action '" + action + "' is not in dictionary");
    }

    // Validate entities (used in cache key)
    if (entities != null) {
      for (String entity : entities) {
        if (!dictionary.hasEntity(entity)) {
          errors.add("Entity '" + entity + "' is not in dictionary");
        }
      }
    }

    // Validate params keys (used in cache key)
    if (params != null && !params.isEmpty()) {
      for (String paramKey : params.keySet()) {
        if (!dictionary.hasField(paramKey)) {
          errors.add("Param key '" + paramKey + "' is not in dictionary");
        }
      }
    }

    // Validate group_by fields (used in cache key, order is significant)
    if (groupBy != null && !groupBy.isEmpty()) {
      for (String field : groupBy) {
        if (!dictionary.hasField(field)) {
          errors.add("Group by field '" + field + "' is not in dictionary");
        }
      }
    }

    return errors;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PromptSchema that = (PromptSchema) o;
    return Objects.equals(action, that.action)
        && Objects.equals(entities, that.entities)
        && Objects.equals(groupBy, that.groupBy)
        && Objects.equals(params, that.params);
  }

  @Override
  public int hashCode() {
    return Objects.hash(action, entities, groupBy, params);
  }

  @Override
  public String toString() {
    return "PromptSchema{"
        + "action='"
        + action
        + '\''
        + ", entities="
        + entities
        + ", groupBy="
        + groupBy
        + ", params="
        + params
        + '}';
  }
}
