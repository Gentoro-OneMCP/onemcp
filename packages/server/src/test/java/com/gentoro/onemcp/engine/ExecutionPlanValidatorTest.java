package com.gentoro.onemcp.engine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ExecutionPlanValidator} with the latest spec only. */
class ExecutionPlanValidatorTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  @DisplayName("valid minimal plan (new spec) passes validation")
  void validPlanPasses() throws Exception {
    String json =
        """
        {
          "start_node": { "route": "op" },
          "op": { "operation": "noop", "route": "out" },
          "out": { "completed": true, "vars": {"ok": true} }
        }
        """;
    JsonNode plan = mapper.readTree(json);
    assertDoesNotThrow(() -> ExecutionPlanValidator.validate(plan));
  }

  @Test
  @DisplayName("missing start_node object fails")
  void missingStartNodeObjectFails() throws Exception {
    JsonNode plan = mapper.readTree("{}\n");
    assertThrows(ExecutionPlanException.class, () -> ExecutionPlanValidator.validate(plan));
  }

  @Test
  @DisplayName("start_node must be an object")
  void startNodeMustBeObject() throws Exception {
    String json = """
        {
          "start_node": "op"
        }
        """;
    JsonNode plan = mapper.readTree(json);
    assertThrows(ExecutionPlanException.class, () -> ExecutionPlanValidator.validate(plan));
  }

  @Test
  @DisplayName("non-terminal node must define operation")
  void nonTerminalNodeWithoutOperationFails() throws Exception {
    String json =
        """
        {
          "start_node": { "route": "bad" },
          "bad": { "route": "out" },
          "out": { "completed": true }
        }
        """;
    JsonNode plan = mapper.readTree(json);
    assertThrows(ExecutionPlanException.class, () -> ExecutionPlanValidator.validate(plan));
  }

  @Test
  @DisplayName("route must reference existing target nodes")
  void routeMustReferenceExistingNodes() throws Exception {
    String json =
        """
        {
          "start_node": { "route": "op" },
          "op": { "operation": "noop", "route": "missing" }
        }
        """;
    JsonNode plan = mapper.readTree(json);
    assertThrows(ExecutionPlanException.class, () -> ExecutionPlanValidator.validate(plan));
  }
}
