package com.gentoro.onemcp.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ToolProperty {
  public enum Type {
    STRING,
    BOOLEAN,
    INTEGER,
    NUMBER,
    OBJECT,
    ARRAY
  }

  private String name;
  private String description;
  private boolean required;
  private Type type;
  private ToolProperty items;
  private List<ToolProperty> properties;

  public ToolProperty(String name, String description, Type type) {
    this(name, description, false, type, null, null);
  }

  public ToolProperty(String name, String description, boolean required, Type type) {
    this(name, description, required, type, null, null);
  }

  public ToolProperty(
      String name,
      String description,
      boolean required,
      Type type,
      ToolProperty items,
      List<ToolProperty> properties) {
    this.name = name;
    this.description = description;
    this.required = required;
    this.type = type;
    this.items = items;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isRequired() {
    return required;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public ToolProperty getItems() {
    return items;
  }

  public void setItems(ToolProperty items) {
    this.items = items;
  }

  public List<ToolProperty> getProperties() {
    return properties;
  }

  public void setProperties(List<ToolProperty> properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ToolProperty that)) return false;
    return isRequired() == that.isRequired()
        && Objects.equals(getName(), that.getName())
        && Objects.equals(getDescription(), that.getDescription())
        && getType() == that.getType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getDescription(), isRequired(), getType());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private String description;
    private boolean required;
    private Type type;
    private ToolProperty items;
    private List<ToolProperty> properties;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder required(boolean required) {
      this.required = required;
      return this;
    }

    public Builder type(Type type) {
      this.type = type;
      return this;
    }

    public Builder items(ToolProperty items) {
      this.items = items;
      return this;
    }

    public Builder properties(List<ToolProperty> properties) {
      this.properties = properties;
      return this;
    }

    public Builder property(ToolProperty property) {
      if (this.properties == null) {
        this.properties = new ArrayList<>();
      }
      this.properties.add(property);
      return this;
    }

    public ToolProperty build() {
      return new ToolProperty(name, description, required, type, items, properties);
    }
  }
}
