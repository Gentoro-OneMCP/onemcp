package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Conceptual Lexicon - A flat vocabulary for expressing user intent.
 * 
 * <p>The lexicon defines the vocabulary available to the normalizer:
 * - actions: what can be done (search, update, create, delete)
 * - entities: what things exist (sale, customer, product)
 * - fields: conceptual value types with optional suffixes for disambiguation
 * 
 * <p>Key principles:
 * - Lexicon is FLAT - no table-column relationships
 * - Fields are conceptual value types, not physical schema fields
 * - Entity prefixes are NOT in the lexicon (normalizer adds entity.field)
 * - Suffixes disambiguate format: date_yyyy vs date_yyyy_mm_dd
 */
public class ConceptualLexicon {
  
  @JsonProperty("actions")
  private List<String> actions = new ArrayList<>();
  
  @JsonProperty("entities")
  private List<String> entities = new ArrayList<>();
  
  @JsonProperty("fields")
  private List<String> fields = new ArrayList<>();
  
  public ConceptualLexicon() {}
  
  public ConceptualLexicon(List<String> actions, List<String> entities, List<String> fields) {
    this.actions = actions != null ? new ArrayList<>(actions) : new ArrayList<>();
    this.entities = entities != null ? new ArrayList<>(entities) : new ArrayList<>();
    this.fields = fields != null ? new ArrayList<>(fields) : new ArrayList<>();
  }
  
  public List<String> getActions() {
    return actions;
  }
  
  public void setActions(List<String> actions) {
    this.actions = actions;
  }
  
  public List<String> getEntities() {
    return entities;
  }
  
  public void setEntities(List<String> entities) {
    this.entities = entities;
  }
  
  public List<String> getFields() {
    return fields;
  }
  
  public void setFields(List<String> fields) {
    this.fields = fields;
  }
  
  public void addAction(String action) {
    if (!actions.contains(action)) {
      actions.add(action);
    }
  }
  
  public void addEntity(String entity) {
    if (!entities.contains(entity)) {
      entities.add(entity);
    }
  }
  
  public void addField(String field) {
    if (!fields.contains(field)) {
      fields.add(field);
    }
  }
  
  /**
   * Sort all lists for deterministic output.
   */
  public void sort() {
    java.util.Collections.sort(actions);
    java.util.Collections.sort(entities);
    java.util.Collections.sort(fields);
  }
  
  @Override
  public String toString() {
    return "ConceptualLexicon{" +
        "actions=" + actions.size() +
        ", entities=" + entities.size() +
        ", fields=" + fields.size() +
        '}';
  }
}

