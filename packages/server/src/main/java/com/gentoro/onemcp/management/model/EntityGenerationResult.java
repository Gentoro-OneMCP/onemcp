package com.gentoro.onemcp.management.model;

import java.util.ArrayList;
import java.util.List;

public class EntityGenerationResult {

  private List<Entity> entities;

  public EntityGenerationResult() {
    entities = new ArrayList<>();
  }

  public List<Entity> getEntities() {
    return entities;
  }

  public void setEntities(List<Entity> entities) {
    this.entities = entities;
  }

  // ======================================================
  //                      ENTITY
  // ======================================================
  public static class Entity {
    private String entityName; // {single-word entity}
    private String description; // detailed definition
    private List<Operation> operations; // operations[]
    private List<Relationship> relationships; // relationships[]

    public Entity() {
      operations = new ArrayList<>();
      relationships = new ArrayList<>();
    }

    public String getEntityName() {
      return entityName;
    }

    public void setEntityName(String entityName) {
      this.entityName = entityName;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public List<Operation> getOperations() {
      return operations;
    }

    public void setOperations(List<Operation> operations) {
      this.operations = operations;
    }

    public List<Relationship> getRelationships() {
      return relationships;
    }

    public void setRelationships(List<Relationship> relationships) {
      this.relationships = relationships;
    }
  }

  // ======================================================
  //                     OPERATION
  // ======================================================
  public static class Operation {
    private String operationType; // Retrieve | Compute | Create | Update | Delete
    private List<ApiPath> apiPaths;
    private String definition; // explanation of operation

    public Operation() {
      apiPaths = new ArrayList<>();
    }

    public String getOperationType() {
      return operationType;
    }

    public void setOperationType(String operationType) {
      this.operationType = operationType;
    }

    public List<ApiPath> getApiPaths() {
      return apiPaths;
    }

    public void setApiPaths(List<ApiPath> apiPaths) {
      this.apiPaths = apiPaths;
    }

    public String getDefinition() {
      return definition;
    }

    public void setDefinition(String definition) {
      this.definition = definition;
    }
  }

  // ======================================================
  //                     API PATH
  // ======================================================
  public static class ApiPath {
    private String path; // "{path described at the API}"
    private String httpMethod; // GET | POST | PUT | PATCH | DELETE

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public String getHttpMethod() {
      return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
      this.httpMethod = httpMethod;
    }
  }

  // ======================================================
  //                   RELATIONSHIP
  // ======================================================
  public static class Relationship {
    private String entityName; // related entity name
    private String relationKind; // toOne | toMany
    private String relation; // description of relationship

    public String getEntityName() {
      return entityName;
    }

    public void setEntityName(String entityName) {
      this.entityName = entityName;
    }

    public String getRelationKind() {
      return relationKind;
    }

    public void setRelationKind(String relationKind) {
      this.relationKind = relationKind;
    }

    public String getRelation() {
      return relation;
    }

    public void setRelation(String relation) {
      this.relation = relation;
    }
  }
}
