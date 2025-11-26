package com.gentoro.onemcp.handbook.model.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Alias {

  private List<String> terms;
  private String description;

  public Alias() {
    this.terms = new ArrayList<>();
  }

  public Alias(String name) {
    this();
    this.terms.add(name);
  }

  public List<String> getTerms() {
    return terms;
  }

  public void setTerms(List<String> terms) {
    this.terms = new ArrayList<>(Objects.requireNonNullElse(terms, Collections.emptyList()));
    ;
  }

  public void addTerm(String term) {
    terms.add(term);
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
