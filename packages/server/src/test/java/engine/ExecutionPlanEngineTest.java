package engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.ExecutionPlanException;
import com.gentoro.onemcp.engine.OperationRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link ExecutionPlanEngine} using the latest spec (start_node object +
 * top-level nodes).
 */
class ExecutionPlanEngineTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private ExecutionPlanEngine newEngine(OperationRegistry registry) {
    return new ExecutionPlanEngine(mapper, registry);
  }

  @Test
  @DisplayName("basic two-node flow with terminal vars")
  void basicTwoNodeFlow() throws Exception {
    OperationRegistry registry = new OperationRegistry().register("echo", input -> input);

    String planJson =
        """
        {
          "start_node": { "route": "op1" },
          "op1": { "operation": "echo", "input": {"greet": "hello"}, "route": "op2" },
          "op2": { "operation": "echo", "input": {"msg": "$.op1.greet"}, "route": "out" },
          "out": { "completed": true, "vars": { "final": "$.op2.msg" } }
        }
        """;

    JsonNode plan = mapper.readTree(planJson);
    JsonNode result = newEngine(registry).execute(plan, null);
    assertEquals("hello", result.get("final").asText());
  }

  @Test
  @DisplayName("routing with condition array picks the true branch")
  void routingWithConditionArray() throws Exception {
    OperationRegistry registry =
        new OperationRegistry()
            .register(
                "seed",
                in -> {
                  try {
                    return mapper.readTree("{\"flag\":true,\"message\":\"ok\"}");
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });

    String planJson =
        """
        {
          "start_node": { "route": "seed" },
          "seed": {
            "operation": "seed",
            "route": [
              {"condition": "$.seed.flag", "node": "T"},
              "F"
            ]
          },
          "T": { "completed": true, "vars": { "result": "$.seed.message" } },
          "F": { "completed": true, "vars": { "result": "no" } }
        }
        """;

    JsonNode plan = mapper.readTree(planJson);
    JsonNode result = newEngine(registry).execute(plan, null);
    assertEquals("ok", result.get("result").asText());
  }

  @Test
  @DisplayName("happy path single operation to terminal")
  void happyPathCall() throws Exception {
    OperationRegistry registry =
        new OperationRegistry().register("echo", input -> input.get("value"));

    String planJson =
        """
        {
          "start_node": { "route": "op" },
          "op": { "operation": "echo", "input": {"value": "hello"}, "route": "out" },
          "out": { "completed": true, "vars": { "final": "$.op" } }
        }
        """;

    JsonNode seededPlan = mapper.readTree(planJson);

    JsonNode result = newEngine(registry).execute(seededPlan, null);
    assertEquals("hello", result.get("final").asText());
  }

  @Test
  @DisplayName("route object form must have exactly one entry with textual value")
  void invalidRouteObjectFails() throws Exception {
    OperationRegistry registry = new OperationRegistry().register("noop", in -> in);

    String planJson =
        """
        {
          "start_node": { "route": "step" },
          "step": { "operation": "noop", "route": {"a": "x", "b": "y"} },
          "x": { "completed": true, "vars": {"ok": "x"} },
          "y": { "completed": true, "vars": {"ok": "y"} }
        }
        """;

    JsonNode plan = mapper.readTree(planJson);
    ExecutionPlanEngine engine = newEngine(registry);
    assertThrows(ExecutionPlanException.class, () -> engine.execute(plan, null));
  }

  @Test
  @DisplayName("missing operation in registry results in ExecutionPlanException")
  void missingOperationFails() throws Exception {
    OperationRegistry registry = new OperationRegistry();

    String planJson =
        """
        {
          "start_node": { "route": "op" },
          "op": { "operation": "unknown", "route": "out" },
          "out": { "completed": true, "vars": {"noop": 1} }
        }
        """;

    JsonNode plan = mapper.readTree(planJson);
    ExecutionPlanEngine engine = newEngine(registry);

    assertThrows(
        ExecutionPlanException.class, () -> engine.execute(plan, mapper.createObjectNode()));
  }

  @Test
  @DisplayName("route array uses fallback when no condition matches")
  void routeArrayUsesFallback() throws Exception {
    OperationRegistry registry =
        new OperationRegistry()
            .register(
                "seed",
                in -> {
                  try {
                    return mapper.readTree("{\"flag\":false}");
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });

    String planJson =
        """
        {
          "start_node": { "route": "seed" },
          "seed": { "operation": "seed", "route": [ {"condition": "$.seed.flag", "node": "A"}, "B" ] },
          "A": { "completed": true, "vars": {"picked": "A"} },
          "B": { "completed": true, "vars": {"picked": "B"} }
        }
        """;

    JsonNode plan = mapper.readTree(planJson);
    JsonNode result = newEngine(registry).execute(plan, null);
    assertEquals("B", result.get("picked").asText());
  }
}
