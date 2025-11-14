package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.utility.JacksonUtility;

public class HttpUtils {
  public static JsonNode toJsonNode(String json) throws Exception {
    if (json == null || json.isEmpty()) {
      return JacksonUtility.getJsonMapper().createObjectNode();
    }
    return JacksonUtility.getJsonMapper().readTree(json);
  }
}
