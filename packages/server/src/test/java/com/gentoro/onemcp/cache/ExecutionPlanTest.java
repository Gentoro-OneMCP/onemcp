package com.gentoro.onemcp.cache;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ExecutionPlan - the first-class execution plan object.
 */
class ExecutionPlanTest {

  @Test
  void testFromJson_validJson() {
    String json = """
        {
          "start_node": {
            "vars": { "year": "2024" },
            "route": "summary"
          },
          "summary": {
            "completed": true,
            "vars": { "answer": "Test result" }
          }
        }
        """;

    ExecutionPlan plan = ExecutionPlan.fromJson(json);
    assertNotNull(plan);
    assertFalse(plan.hasPlaceholders());
  }

  @Test
  void testFromJson_invalidJson() {
    String invalidJson = "{ invalid json }";
    assertThrows(IllegalArgumentException.class, () -> ExecutionPlan.fromJson(invalidJson));
  }

  @Test
  void testFromJson_nullJson() {
    assertThrows(IllegalArgumentException.class, () -> ExecutionPlan.fromJson(null));
  }

  @Test
  void testFromJson_emptyJson() {
    assertThrows(IllegalArgumentException.class, () -> ExecutionPlan.fromJson(""));
  }

  @Test
  void testHasPlaceholders_withPlaceholders() {
    String json = """
        {
          "start_node": {
            "vars": { "year": "{{params.year}}" },
            "route": "summary"
          },
          "summary": {
            "completed": true,
            "vars": { "answer": "Result for year {{params.year}}" }
          }
        }
        """;

    ExecutionPlan plan = ExecutionPlan.fromJson(json);
    assertTrue(plan.hasPlaceholders());
  }

  @Test
  void testHasPlaceholders_withoutPlaceholders() {
    String json = """
        {
          "start_node": {
            "vars": { "year": "2024" },
            "route": "summary"
          },
          "summary": {
            "completed": true,
            "vars": { "answer": "Static result" }
          }
        }
        """;

    ExecutionPlan plan = ExecutionPlan.fromJson(json);
    assertFalse(plan.hasPlaceholders());
  }

  @Test
  void testToJson_roundTrip() {
    String originalJson = """
        {"start_node":{"vars":{"year":"2024"},"route":"summary"},"summary":{"completed":true,"vars":{"answer":"Test"}}}
        """.trim();

    ExecutionPlan plan = ExecutionPlan.fromJson(originalJson);
    String serialized = plan.toJson();
    
    // Re-parse to verify it's valid
    ExecutionPlan reparsed = ExecutionPlan.fromJson(serialized);
    assertNotNull(reparsed);
  }

  @Test
  void testToPrettyJson() {
    String json = """
        {"start_node":{"vars":{"year":"2024"},"route":"summary"},"summary":{"completed":true}}
        """;

    ExecutionPlan plan = ExecutionPlan.fromJson(json);
    String pretty = plan.toPrettyJson();
    
    // Pretty JSON should contain newlines
    assertTrue(pretty.contains("\n"));
    // Should still be parseable
    ExecutionPlan reparsed = ExecutionPlan.fromJson(pretty);
    assertNotNull(reparsed);
  }
}


