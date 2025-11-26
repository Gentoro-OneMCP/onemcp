package com.gentoro.onemcp.engine;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link JsonPathResolver}. */
class JsonPathResolverTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonPathResolver resolver = new JsonPathResolver(mapper);

  @Test
  @DisplayName("simple field resolution works")
  void simplePathResolves() {
    ObjectNode root = mapper.createObjectNode();
    root.put("value", 42);

    JsonNode result = resolver.read(root, "$.value");
    assertNotNull(result);
    assertEquals(42, result.asInt());
  }

  @Test
  @DisplayName("invalid JsonPath throws ExecutionPlanException")
  void invalidPathThrows() {
    ObjectNode root = mapper.createObjectNode();
    root.put("value", 1);

    // Use an empty JsonPath expression which our resolver explicitly rejects and
    // wraps in an ExecutionPlanException.
    assertThrows(ExecutionPlanException.class, () -> resolver.read(root, ""));
  }
}
