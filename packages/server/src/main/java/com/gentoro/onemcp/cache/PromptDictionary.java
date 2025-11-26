package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Canonical dictionary for Prompt Schema normalization.
 *
 * <p>Contains the allowed vocabulary extracted from API specifications:
 * actions, entities, fields, operators, and aggregation functions.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptDictionary {
  @JsonProperty("actions")
  private List<String> actions = new ArrayList<>();

  @JsonProperty("entities")
  private List<String> entities = new ArrayList<>();

  @JsonProperty("fields")
  private List<String> fields = new ArrayList<>();

  @JsonProperty("operators")
  private List<String> operators = new ArrayList<>();

  @JsonProperty("aggregates")
  private List<String> aggregates = new ArrayList<>();

  public PromptDictionary() {}

  public List<String> getActions() {
    return actions;
  }

  public void setActions(List<String> actions) {
    this.actions = actions != null ? actions : new ArrayList<>();
  }

  public List<String> getEntities() {
    return entities;
  }

  public void setEntities(List<String> entities) {
    this.entities = entities != null ? entities : new ArrayList<>();
  }

  public List<String> getFields() {
    return fields;
  }

  public void setFields(List<String> fields) {
    this.fields = fields != null ? fields : new ArrayList<>();
  }

  public List<String> getOperators() {
    return operators;
  }

  public void setOperators(List<String> operators) {
    this.operators = operators != null ? operators : new ArrayList<>();
  }

  public List<String> getAggregates() {
    return aggregates;
  }

  public void setAggregates(List<String> aggregates) {
    this.aggregates = aggregates != null ? aggregates : new ArrayList<>();
  }

  /**
   * Check if an action is in the dictionary.
   */
  public boolean hasAction(String action) {
    return actions.contains(action);
  }

  /**
   * Check if an entity is in the dictionary.
   */
  public boolean hasEntity(String entity) {
    return entities.contains(entity);
  }

  /**
   * Check if a field is in the dictionary.
   */
  public boolean hasField(String field) {
    return fields.contains(field);
  }

  /**
   * Check if an operator is in the dictionary.
   */
  public boolean hasOperator(String operator) {
    return operators.contains(operator);
  }

  /**
   * Check if an aggregate function is in the dictionary.
   */
  public boolean hasAggregate(String aggregate) {
    return aggregates.contains(aggregate);
  }
}

