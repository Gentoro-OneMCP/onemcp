package com.gentoro.onemcp.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Prompt Schema Key (PSK) - Deterministic cache key generated from a Prompt Schema.
 *
 * <p>According to cache_spec.txt, the PSK is generated from:
 * <ul>
 *   <li>action
 *   <li>entity (singular)
 *   <li>Set of filter fields and operators
 *   <li>Set of param field names
 *   <li>Set of shape (group_by, aggregates, order_by) field names
 *   <li>Presence of limit / offset (but not their numeric values)
 * </ul>
 *
 * <p>Note: param values are NOT included in the PSK, allowing cache reuse across different
 * parameter values for the same schema structure.
 *
 * <p>The key can be represented as:
 *
 * <ul>
 *   <li>Human-readable string format
 *   <li>SHA-256 hash for storage efficiency
 * </ul>
 */
public class PromptSchemaKey {
  private final String action;
  private final String entity; // Singular, not entities array
  private final List<String> filterFields; // Sorted set of filter field names
  private final List<String> filterOperators; // Sorted set of filter operators
  private final List<String> paramFields; // Sorted set of param field names
  private final List<String> shapeFields; // Sorted set of shape field names (group_by, aggregates, order_by)
  private final boolean hasLimit; // Presence, not value
  private final boolean hasOffset; // Presence, not value
  private final String stringKey;
  private final String hashKey;

  /**
   * Create a PSK from a PromptSchema.
   *
   * @param ps the PromptSchema to generate a key from
   */
  public PromptSchemaKey(PromptSchema ps) {
    if (ps == null) {
      throw new IllegalArgumentException("PromptSchema cannot be null");
    }

    this.action = ps.getAction() != null ? ps.getAction() : "";
    this.entity = ps.getEntity() != null ? ps.getEntity() : "";

    // Extract filter fields and operators (sorted sets)
    List<String> filterFieldList = new ArrayList<>();
    List<String> filterOpList = new ArrayList<>();
    if (ps.getFilter() != null) {
      for (FilterExpression fe : ps.getFilter()) {
        if (fe != null) {
          if (fe.getField() != null) {
            filterFieldList.add(fe.getField());
          }
          if (fe.getOperator() != null) {
            filterOpList.add(fe.getOperator());
          }
        }
      }
    }
    Collections.sort(filterFieldList);
    Collections.sort(filterOpList);
    this.filterFields = Collections.unmodifiableList(filterFieldList);
    this.filterOperators = Collections.unmodifiableList(filterOpList);

    // Sort params.keys() alphabetically for deterministic key
    List<String> sortedParamFields = new ArrayList<>();
    if (ps.getParams() != null && !ps.getParams().isEmpty()) {
      sortedParamFields.addAll(ps.getParams().keySet());
      Collections.sort(sortedParamFields);
    }
    this.paramFields = Collections.unmodifiableList(sortedParamFields);

    // Extract shape fields (group_by, aggregates, order_by) - sorted set
    List<String> shapeFieldList = new ArrayList<>();
    boolean hasLimitFlag = false;
    boolean hasOffsetFlag = false;
    if (ps.getShape() != null) {
      PromptSchemaShape shape = ps.getShape();
      // group_by fields
      if (shape.getGroupBy() != null) {
        shapeFieldList.addAll(shape.getGroupBy());
    }
      // aggregate fields
      if (shape.getAggregates() != null) {
        for (PromptSchemaShape.AggregateSpec agg : shape.getAggregates()) {
          if (agg != null && agg.getField() != null) {
            shapeFieldList.add(agg.getField());
          }
        }
      }
      // order_by fields
      if (shape.getOrderBy() != null) {
        for (PromptSchemaShape.OrderSpec order : shape.getOrderBy()) {
          if (order != null && order.getField() != null) {
            shapeFieldList.add(order.getField());
          }
        }
      }
      // limit/offset presence (not values)
      hasLimitFlag = shape.getLimit() != null;
      hasOffsetFlag = shape.getOffset() != null;
    }
    Collections.sort(shapeFieldList);
    this.shapeFields = Collections.unmodifiableList(shapeFieldList);
    this.hasLimit = hasLimitFlag;
    this.hasOffset = hasOffsetFlag;

    // Generate human-readable string key
    this.stringKey = generateStringKey();

    // Generate hash key for storage efficiency
    this.hashKey = generateHashKey();
  }

  /** Create a PSK from explicit components (for testing or manual construction). */
  public PromptSchemaKey(
      String action,
      String entity,
      List<String> filterFields,
      List<String> filterOperators,
      List<String> paramFields,
      List<String> shapeFields,
      boolean hasLimit,
      boolean hasOffset) {
    this.action = action != null ? action : "";
    this.entity = entity != null ? entity : "";

    List<String> sortedFilterFields = new ArrayList<>();
    if (filterFields != null) {
      sortedFilterFields.addAll(filterFields);
      Collections.sort(sortedFilterFields);
    }
    this.filterFields = Collections.unmodifiableList(sortedFilterFields);

    List<String> sortedFilterOps = new ArrayList<>();
    if (filterOperators != null) {
      sortedFilterOps.addAll(filterOperators);
      Collections.sort(sortedFilterOps);
    }
    this.filterOperators = Collections.unmodifiableList(sortedFilterOps);

    List<String> sortedParamFields = new ArrayList<>();
    if (paramFields != null) {
      sortedParamFields.addAll(paramFields);
      Collections.sort(sortedParamFields);
    }
    this.paramFields = Collections.unmodifiableList(sortedParamFields);

    List<String> sortedShapeFields = new ArrayList<>();
    if (shapeFields != null) {
      sortedShapeFields.addAll(shapeFields);
      Collections.sort(sortedShapeFields);
    }
    this.shapeFields = Collections.unmodifiableList(sortedShapeFields);

    this.hasLimit = hasLimit;
    this.hasOffset = hasOffset;

    this.stringKey = generateStringKey();
    this.hashKey = generateHashKey();
  }

  private String generateStringKey() {
    // Generate filename-safe cache key
    // Format: action-entity-filterFields_filterOps-paramFields-shapeFields-limit_offset
    // Uses hyphens (-) to separate components, underscores (_) within components
    StringBuilder sb = new StringBuilder();
    sb.append(action != null ? action : "");

    if (!entity.isEmpty()) {
      sb.append("-");
      sb.append(entity);
    }

    if (!filterFields.isEmpty()) {
      sb.append("-filter_");
      sb.append(String.join("_", filterFields));
      if (!filterOperators.isEmpty()) {
        sb.append("_ops_");
        sb.append(String.join("_", filterOperators));
      }
    }

    if (!paramFields.isEmpty()) {
      sb.append("-params_");
      sb.append(String.join("_", paramFields));
    }

    if (!shapeFields.isEmpty()) {
      sb.append("-shape_");
      sb.append(String.join("_", shapeFields));
    }

    if (hasLimit || hasOffset) {
      sb.append("-");
      if (hasLimit) sb.append("limit");
      if (hasLimit && hasOffset) sb.append("_");
      if (hasOffset) sb.append("offset");
    }

    return sb.toString();
  }

  private String generateHashKey() {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(stringKey.getBytes(StandardCharsets.UTF_8));
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not available", e);
    }
  }

  public String getAction() {
    return action;
  }

  public String getEntity() {
    return entity;
  }

  public List<String> getFilterFields() {
    return filterFields;
  }

  public List<String> getFilterOperators() {
    return filterOperators;
  }

  public List<String> getParamFields() {
    return paramFields;
  }

  public List<String> getShapeFields() {
    return shapeFields;
  }

  public boolean hasLimit() {
    return hasLimit;
  }

  public boolean hasOffset() {
    return hasOffset;
  }

  /** Get the human-readable string representation of the key. */
  public String getStringKey() {
    return stringKey;
  }

  /** Get the SHA-256 hash representation of the key (for storage efficiency). */
  public String getHashKey() {
    return hashKey;
  }

  /** Get the key to use for cache storage (defaults to hash for efficiency). */
  public String getCacheKey() {
    return hashKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PromptSchemaKey that = (PromptSchemaKey) o;
    return Objects.equals(action, that.action)
        && Objects.equals(entity, that.entity)
        && Objects.equals(filterFields, that.filterFields)
        && Objects.equals(filterOperators, that.filterOperators)
        && Objects.equals(paramFields, that.paramFields)
        && Objects.equals(shapeFields, that.shapeFields)
        && hasLimit == that.hasLimit
        && hasOffset == that.hasOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        action, entity, filterFields, filterOperators, paramFields, shapeFields, hasLimit, hasOffset);
  }

  @Override
  public String toString() {
    return "PromptSchemaKey{"
        + "stringKey='"
        + stringKey
        + '\''
        + ", hashKey='"
        + hashKey
        + '\''
        + '}';
  }
}
