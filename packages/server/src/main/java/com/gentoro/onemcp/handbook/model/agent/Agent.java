package com.gentoro.onemcp.handbook.model.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Agent {
  private List<Release> releases;
  private List<Api> apis;
  private Guardrails guardrails;

  public Agent() {
    this.releases = new ArrayList<>(List.of(new Release()));
    this.apis = new ArrayList<>();
    this.guardrails = new Guardrails();
  }

  public Guardrails getGuardrails() {
    return guardrails;
  }

  public void setGuardrails(Guardrails guardrails) {
    this.guardrails = guardrails;
  }

  public List<Release> getReleases() {
    return releases;
  }

  public void setReleases(List<Release> releases) {
    this.releases = releases;
  }

  public List<Api> getApis() {
    return Collections.unmodifiableList(apis);
  }

  public void setApis(List<Api> apis) {
    this.apis = new ArrayList<>(Objects.requireNonNullElse(apis, Collections.emptyList()));
  }

  public void addApi(Api api) {
    apis.add(api);
  }
}
