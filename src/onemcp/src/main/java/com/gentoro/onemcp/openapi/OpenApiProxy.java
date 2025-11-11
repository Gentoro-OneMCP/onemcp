package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;

public interface OpenApiProxy {
  JsonNode invoke(String operationId, JsonNode input) throws Exception;
}
