package com.gentoro.onemcp.handbook.model.agent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.gentoro.onemcp.handbook.Handbook;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Api {
  @JsonIgnore private transient Handbook handbook;
  @JsonIgnore private transient Service service;
  private String slug;
  private String name;
  private String ref;
  private String description;
  private List<Entity> entities;
  private List<String> baseUrls;

  public Api() {
    this.entities = new ArrayList<>();
    this.baseUrls = new ArrayList<>();
  }

  public String getSlug() {
    return slug;
  }

  public void setSlug(String slug) {
    this.slug = slug;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getRef() {
    return ref;
  }

  public void setRef(String ref) {
    this.ref = ref;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<Entity> getEntities() {
    return entities;
  }

  public void setEntities(List<Entity> entities) {
    this.entities = new ArrayList<>(Objects.requireNonNullElse(entities, Collections.emptyList()));
    ;
  }

  public void addEntity(Entity entity) {
    entities.add(entity);
  }

  public void clearEntities() {
    entities.clear();
  }

  public List<String> getBaseUrls() {
    return baseUrls;
  }

  public void setBaseUrls(List<String> baseUrls) {
    this.baseUrls = new ArrayList<>(Objects.requireNonNullElse(baseUrls, Collections.emptyList()));
    ;
  }

  public void addBaseUrl(String baseUrl) {
    baseUrls.add(baseUrl);
  }

  public void bindHandbook(Handbook handbook) {
    this.handbook = handbook;
    this.service = new Service(this);
  }

  public void bindHandbook(Handbook handbook, Service service) {
    this.handbook = handbook;
    this.service = service;
  }

  public Handbook getHandbook() {
    return handbook;
  }

  public Service getService() {
    return service;
  }
}
