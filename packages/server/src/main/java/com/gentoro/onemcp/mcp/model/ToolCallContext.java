package com.gentoro.onemcp.mcp.model;

import com.gentoro.onemcp.exception.ValidationException;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.List;
import java.util.Objects;

public class ToolCallContext {
  private List<ContextItem> context;

  public List<ContextItem> getContext() {
    return context;
  }

  public void setContext(List<ContextItem> context) {
    this.context = context;
  }

  public static ToolCallContext valueOf(String jsonString) {
    try {
      if (Objects.nonNull(jsonString) && !jsonString.isEmpty()) {
        return JacksonUtility.getJsonMapper().readValue(jsonString, ToolCallContext.class);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new ValidationException("Failed to parse RootContext", e);
    }
  }
}
