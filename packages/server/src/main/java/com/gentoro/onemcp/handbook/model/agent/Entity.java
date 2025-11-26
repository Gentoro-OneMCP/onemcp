package com.gentoro.onemcp.handbook.model.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Entity {

  private String name;
  private List<Alias> aliases;
  private String openApiTag;
  private String description;
  private List<Relationship> relationships;
  private List<EntityOperation> operations;

  public Entity() {
    // default constructor, used by Jackson
    this.aliases = new ArrayList<>();
    this.relationships = new ArrayList<>();
    this.operations = new ArrayList<>();
  }

  public Entity(String name) {
    this();
    this.name = name;
    this.aliases.add(new Alias(name));
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Alias> getAliases() {
    return aliases;
  }

  public void setAliases(List<Alias> aliases) {
    this.aliases = aliases;
  }

  public String getOpenApiTag() {
    return openApiTag;
  }

  public void setOpenApiTag(String openApiTag) {
    this.openApiTag = openApiTag;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<Relationship> getRelationships() {
    return relationships;
  }

  public void setRelationships(List<Relationship> relationships) {
    this.relationships =
        new ArrayList<>(Objects.requireNonNullElse(relationships, Collections.emptyList()));
    ;
  }

  public void addRelationship(Relationship relationship) {
    relationships.add(relationship);
  }

  public List<EntityOperation> getOperations() {
    return operations;
  }

  public void setOperations(List<EntityOperation> operations) {
    this.operations =
        new ArrayList<>(Objects.requireNonNullElse(operations, Collections.emptyList()));
    ;
  }

  public void addOperation(EntityOperation operation) {
    operations.add(operation);
  }
}
