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
 * <p>The PSK is generated from: (action, sorted(entities), sorted(params.keys()), group_by) where
 * group_by is in declared order (not sorted).
 *
 * <p>Note: param values are NOT included in the PSK, allowing cache reuse across different
 * parameter values for the same schema structure.
 *
 * <p>The key can be represented as:
 *
 * <ul>
 *   <li>Human-readable string format: "action:entity1,entity2:field1,field2:group1,group2"
 *   <li>SHA-256 hash for storage efficiency
 * </ul>
 */
public class PromptSchemaKey {
  private final String action;
  private final List<String> entities;
  private final List<String> fields; // from params.keys()
  private final List<String> groupBy; // in declared order
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

    // Sort entities alphabetically for deterministic key
    List<String> sortedEntities = new ArrayList<>();
    if (ps.getEntities() != null) {
      sortedEntities.addAll(ps.getEntities());
      Collections.sort(sortedEntities);
    }
    this.entities = Collections.unmodifiableList(sortedEntities);

    // Sort params.keys() alphabetically for deterministic key
    List<String> sortedFields = new ArrayList<>();
    if (ps.getParams() != null && !ps.getParams().isEmpty()) {
      sortedFields.addAll(ps.getParams().keySet());
      Collections.sort(sortedFields);
    }
    this.fields = Collections.unmodifiableList(sortedFields);

    // group_by is in declared order (not sorted) - order is significant
    List<String> groupByList = new ArrayList<>();
    if (ps.getGroupBy() != null) {
      groupByList.addAll(ps.getGroupBy());
    }
    this.groupBy = Collections.unmodifiableList(groupByList);

    // Generate human-readable string key
    this.stringKey = generateStringKey();

    // Generate hash key for storage efficiency
    this.hashKey = generateHashKey();
  }

  /** Create a PSK from explicit components (for testing or manual construction). */
  public PromptSchemaKey(
      String action, List<String> entities, List<String> fields, List<String> groupBy) {
    this.action = action != null ? action : "";

    List<String> sortedEntities = new ArrayList<>();
    if (entities != null) {
      sortedEntities.addAll(entities);
      Collections.sort(sortedEntities);
    }
    this.entities = Collections.unmodifiableList(sortedEntities);

    List<String> sortedFields = new ArrayList<>();
    if (fields != null) {
      sortedFields.addAll(fields);
      Collections.sort(sortedFields);
    }
    this.fields = Collections.unmodifiableList(sortedFields);

    List<String> groupByList = new ArrayList<>();
    if (groupBy != null) {
      groupByList.addAll(groupBy); // Keep in declared order
    }
    this.groupBy = Collections.unmodifiableList(groupByList);

    this.stringKey = generateStringKey();
    this.hashKey = generateHashKey();
  }

  private String generateStringKey() {
    // Generate filename-safe cache key
    // Format: action-entity1_entity2-field1_field2-group_field1_field2
    // Uses hyphens (-) to separate components, underscores (_) within components
    // This is filename-safe on all platforms and avoids ambiguity with field names
    StringBuilder sb = new StringBuilder();
    sb.append(action != null ? action : "");

    if (!entities.isEmpty()) {
      sb.append("-");
      sb.append(String.join("_", entities));
    }

    if (!fields.isEmpty()) {
      sb.append("-");
      sb.append(String.join("_", fields));
    }

    if (!groupBy.isEmpty()) {
      sb.append("-group_");
      sb.append(String.join("_", groupBy));
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

  public List<String> getEntities() {
    return entities;
  }

  public List<String> getFields() {
    return fields;
  }

  public List<String> getGroupBy() {
    return groupBy;
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
        && Objects.equals(entities, that.entities)
        && Objects.equals(fields, that.fields)
        && Objects.equals(groupBy, that.groupBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(action, entities, fields, groupBy);
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
