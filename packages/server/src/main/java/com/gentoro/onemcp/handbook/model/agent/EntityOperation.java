package com.gentoro.onemcp.handbook.model.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class EntityOperation {

  private String kind;
  private String description;
  private List<OpenApiRel> openApiRels;

  public EntityOperation() {
    openApiRels = new ArrayList<>();
  }

  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<OpenApiRel> getOpenApiRels() {
    return Collections.unmodifiableList(openApiRels);
  }

  public void setOpenApiRels(List<OpenApiRel> openApiRels) {
    this.openApiRels.addAll(Objects.requireNonNullElse(openApiRels, Collections.emptyList()));
  }

  public void addOpenApiRel(OpenApiRel openApiRel) {
    this.openApiRels.add(openApiRel);
  }
}
