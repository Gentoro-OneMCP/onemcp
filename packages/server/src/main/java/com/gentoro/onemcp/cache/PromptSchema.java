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
 * <p>According to cache_spec.txt, a Prompt Schema contains:
 *
 * <ul>
 *   <li>action: Canonical verb from lexicon.actions (exactly one)
 *   <li>entity: Canonical noun from lexicon.entities (exactly one, singular)
 *   <li>filter: Array of filter expressions (pre-action, optional)
 *   <li>params: Flat object of field → primitive values (action arguments, optional)
 *   <li>shape: Shape object for SQL-like post-action shaping (optional)
 * </ul>
 *
 * <p>The PSK (Prompt Schema Key) is generated from:
 * <ul>
 *   <li>action
 *   <li>entity
 *   <li>Set of filter fields and operators
 *   <li>Set of param field names
 *   <li>Set of shape (group_by, aggregates, order_by) field names
 *   <li>Presence of limit / offset (but not their numeric values)
 * </ul>
 *
 * <p>Param values are excluded for cache reuse across different parameter values.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptSchema {
  @JsonProperty("action")
  private String action;

  @JsonProperty("entity")
  private String entity; // Singular, not entities array

  @JsonProperty("filter")
  private List<FilterExpression> filter = new ArrayList<>();

  @JsonProperty("params")
  private Map<String, Object> params = new HashMap<>(); // Primitive values only

  @JsonProperty("shape")
  private PromptSchemaShape shape; // Optional

  @JsonProperty("cache_key")
  private String cacheKey;

  @JsonProperty("current_timestamp")
  private Long currentTimestamp; // Timestamp when cache key was generated (for relative time calculations)

  @JsonProperty("original_prompt")
  private String originalPrompt; // Original natural language prompt (not used in cache key)

  public PromptSchema() {}

  public PromptSchema(
      String action,
      String entity,
      List<FilterExpression> filter,
      Map<String, Object> params,
      PromptSchemaShape shape) {
    this.action = action;
    this.entity = entity;
    this.filter = filter != null ? new ArrayList<>(filter) : new ArrayList<>();
    this.params = params != null ? new HashMap<>(params) : new HashMap<>();
    this.shape = shape;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }

  public List<FilterExpression> getFilter() {
    return filter;
  }

  public void setFilter(List<FilterExpression> filter) {
    this.filter = filter != null ? new ArrayList<>(filter) : new ArrayList<>();
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public void setParams(Map<String, Object> params) {
    this.params = params != null ? new HashMap<>(params) : new HashMap<>();
  }

  public PromptSchemaShape getShape() {
    return shape;
  }

  public void setShape(PromptSchemaShape shape) {
    this.shape = shape;
  }

  public String getCacheKey() {
    return cacheKey;
  }

  public void setCacheKey(String cacheKey) {
    this.cacheKey = cacheKey;
  }

  public Long getCurrentTimestamp() {
    return currentTimestamp;
  }

  public void setCurrentTimestamp(Long currentTimestamp) {
    this.currentTimestamp = currentTimestamp;
  }

  public String getOriginalPrompt() {
    return originalPrompt;
  }

  public void setOriginalPrompt(String originalPrompt) {
    this.originalPrompt = originalPrompt;
  }

  /**
   * Generate and set the cache key for this schema. The cache key is derived from (action, entity,
   * filter fields/operators, param keys, shape fields, limit/offset presence).
   * Also sets the current_timestamp field for relative time calculations.
   */
  public void generateCacheKey() {
    PromptSchemaKey psk = new PromptSchemaKey(this);
    this.cacheKey = psk.getStringKey(); // Use human-readable format for visibility
    this.currentTimestamp = System.currentTimeMillis(); // Set timestamp when cache key is generated
  }

  /**
   * Validate that cache key components are in the lexicon.
   *
   * @param lexicon the lexicon to validate against
   * @return list of validation errors (empty if valid)
   */
  public List<String> validate(PromptLexicon lexicon) {
    List<String> errors = new ArrayList<>();

    // Validate action (used in cache key)
    if (action == null || action.isEmpty()) {
      errors.add("Action is required for cache key");
    } else if (!"local".equals(action) && !lexicon.hasAction(action)) {
      errors.add("Action '" + action + "' is not in lexicon");
    }

    // Validate entity (used in cache key)
    if (entity == null || entity.isEmpty()) {
      errors.add("Entity is required for cache key");
    } else if (!lexicon.hasEntity(entity)) {
      errors.add("Entity '" + entity + "' is not in lexicon");
    }

    // Validate filter fields (used in cache key)
    if (filter != null) {
      for (FilterExpression fe : filter) {
        if (fe != null && fe.getField() != null) {
          if (!lexicon.hasField(fe.getField())) {
            errors.add("Filter field '" + fe.getField() + "' is not in lexicon");
          }
        }
      }
    }

    // Validate params keys (used in cache key)
    if (params != null && !params.isEmpty()) {
      for (String paramKey : params.keySet()) {
        if (!lexicon.hasField(paramKey)) {
          errors.add("Param key '" + paramKey + "' is not in lexicon");
        }
      }
    }

    // Validate shape fields (used in cache key)
    if (shape != null) {
      // Validate group_by fields
      if (shape.getGroupBy() != null) {
        for (String field : shape.getGroupBy()) {
          if (!lexicon.hasField(field)) {
            errors.add("Group by field '" + field + "' is not in lexicon");
          }
        }
      }
      // Validate aggregate fields
      if (shape.getAggregates() != null) {
        for (PromptSchemaShape.AggregateSpec agg : shape.getAggregates()) {
          if (agg != null && agg.getField() != null) {
            if (!lexicon.hasField(agg.getField())) {
              errors.add("Aggregate field '" + agg.getField() + "' is not in lexicon");
            }
          }
        }
      }
      // Validate order_by fields
      if (shape.getOrderBy() != null) {
        for (PromptSchemaShape.OrderSpec order : shape.getOrderBy()) {
          if (order != null && order.getField() != null) {
            if (!lexicon.hasField(order.getField())) {
              errors.add("Order by field '" + order.getField() + "' is not in lexicon");
            }
          }
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
        && Objects.equals(entity, that.entity)
        && Objects.equals(filter, that.filter)
        && Objects.equals(params, that.params)
        && Objects.equals(shape, that.shape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(action, entity, filter, params, shape);
  }

  @Override
  public String toString() {
    return "PromptSchema{"
        + "action='"
        + action
        + '\''
        + ", entity='"
        + entity
        + '\''
        + ", filter="
        + filter
        + ", params="
        + params
        + ", shape="
        + shape
        + '}';
  }
}
