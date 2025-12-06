package com.gentoro.onemcp.management.model;

import com.gentoro.onemcp.mcp.model.ToolCallContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RegressionRequest {

  private List<String> paths;
  private ToolCallContext context;

  public RegressionRequest() {
    this.paths = new ArrayList<>();
  }

  public List<String> getPaths() {
    return Collections.unmodifiableList(paths);
  }

  public void setPaths(List<String> paths) {
    this.paths.addAll(Objects.requireNonNullElse(paths, Collections.emptyList()));
  }

  public ToolCallContext getContext() {
    return context;
  }

  public void setContext(ToolCallContext context) {
    this.context = context;
  }
}
