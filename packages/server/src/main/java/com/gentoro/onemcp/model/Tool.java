package com.gentoro.onemcp.model;

import java.util.Map;

public interface Tool {
  String name();

  String summary();

  ToolDefinition definition();

  String execute(Map<String, Object> args);
}
