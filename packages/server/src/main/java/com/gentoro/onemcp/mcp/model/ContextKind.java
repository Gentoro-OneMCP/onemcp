package com.gentoro.onemcp.mcp.model;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;

public enum ContextKind {
  API_CONTEXT,

  @JsonEnumDefaultValue
  UNKNOWN
}
