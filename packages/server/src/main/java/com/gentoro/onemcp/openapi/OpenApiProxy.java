package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.mcp.model.ToolCallContext;

public interface OpenApiProxy {
  JsonNode invoke(String operationId, JsonNode input, ToolCallContext toolCallContext)
      throws Exception;
}
