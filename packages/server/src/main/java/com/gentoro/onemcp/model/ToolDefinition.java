package com.gentoro.onemcp.model;

import java.util.Objects;

/**
 * Provider-agnostic tool definition used by agents/tools. It follows a simplified JSON-Schema-like
 * structure for parameters.
 */
public final class ToolDefinition {
  private final String name;
  private final String description;

  /**
   * JSON schema-like map describing parameters, e.g.: { "type": "object", "properties": { ... },
   * "required": [ ... ], "additionalProperties": false }
   */
  private final ToolProperty schema;

  public ToolDefinition(String name, String description, ToolProperty schema) {
    this.name = Objects.requireNonNull(name, "name");
    this.description = Objects.requireNonNull(description, "description");
    this.schema = schema; // may be null
  }

  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  public ToolProperty schema() {
    return schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private String description;
    private ToolProperty schema;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder schema(ToolProperty schema) {
      this.schema = schema;
      return this;
    }

    public ToolDefinition build() {
      return new ToolDefinition(name, description, schema);
    }
  }
}
