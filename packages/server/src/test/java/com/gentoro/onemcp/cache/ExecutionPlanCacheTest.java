package com.gentoro.onemcp.cache;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for ExecutionPlanCache.
 */
class ExecutionPlanCacheTest {

  @TempDir
  Path tempDir;

  private ExecutionPlanCache cache;
  private PromptSchema testSchema;
  private ExecutionPlan testPlan;

  @BeforeEach
  void setUp() {
    cache = new ExecutionPlanCache(tempDir);

    // Create a test schema
    testSchema = new PromptSchema();
    testSchema.setAction("summarize");
    testSchema.setEntities(List.of("sale"));
    testSchema.setGroupBy(List.of("category"));
    Map<String, Object> params = new HashMap<>();
    params.put("year", "2024");
    testSchema.setParams(params);
    testSchema.generateCacheKey();

    // Create a test plan
    String planJson = """
        {
          "start_node": {
            "vars": { "year": "{{params.year}}" },
            "route": "summary"
          },
          "summary": {
            "completed": true,
            "vars": { "answer": "Test result" }
          }
        }
        """;
    testPlan = ExecutionPlan.fromJson(planJson);
  }

  @Test
  void testCacheDirectory_created() {
    Path plansDir = tempDir.resolve("plans");
    assertTrue(Files.exists(plansDir), "Plans directory should be created");
    assertTrue(Files.isDirectory(plansDir), "Plans should be a directory");
  }

  @Test
  void testLookup_notFound() {
    Optional<ExecutionPlan> result = cache.lookup(testSchema);
    assertTrue(result.isEmpty(), "Should return empty for non-existent plan");
  }

  @Test
  void testStoreAndLookup() {
    // Store the plan
    cache.store(testSchema, testPlan);

    // Lookup should find it
    Optional<ExecutionPlan> result = cache.lookup(testSchema);
    assertTrue(result.isPresent(), "Should find the cached plan");
  }

  @Test
  void testExists_notFound() {
    assertFalse(cache.exists(testSchema), "Should return false for non-existent plan");
  }

  @Test
  void testExists_found() {
    cache.store(testSchema, testPlan);
    assertTrue(cache.exists(testSchema), "Should return true for existing plan");
  }

  @Test
  void testRemove_notFound() {
    boolean removed = cache.remove(testSchema);
    assertFalse(removed, "Should return false when removing non-existent plan");
  }

  @Test
  void testRemove_found() {
    cache.store(testSchema, testPlan);
    assertTrue(cache.exists(testSchema), "Plan should exist before removal");

    boolean removed = cache.remove(testSchema);
    assertTrue(removed, "Should return true when removing existing plan");
    assertFalse(cache.exists(testSchema), "Plan should not exist after removal");
  }

  @Test
  void testClear() {
    // Store multiple plans
    cache.store(testSchema, testPlan);

    PromptSchema schema2 = new PromptSchema();
    schema2.setAction("search");
    schema2.setEntities(List.of("customer"));
    schema2.setParams(new HashMap<>());
    schema2.generateCacheKey();
    cache.store(schema2, testPlan);

    assertTrue(cache.exists(testSchema), "First plan should exist");
    assertTrue(cache.exists(schema2), "Second plan should exist");

    // Clear all
    int cleared = cache.clear();
    assertEquals(2, cleared, "Should have cleared 2 plans");
    assertFalse(cache.exists(testSchema), "First plan should be gone");
    assertFalse(cache.exists(schema2), "Second plan should be gone");
  }

  @Test
  void testStore_nullSchema() {
    // Should not throw, just log warning
    assertDoesNotThrow(() -> cache.store(null, testPlan));
  }

  @Test
  void testStore_nullPlan() {
    // Should not throw, just log warning
    assertDoesNotThrow(() -> cache.store(testSchema, null));
  }

  @Test
  void testLookup_nullSchema() {
    Optional<ExecutionPlan> result = cache.lookup(null);
    assertTrue(result.isEmpty(), "Should return empty for null schema");
  }

  @Test
  void testCacheKeyDeterminism() {
    // Two schemas with same structure should have same cache key
    PromptSchema schema1 = new PromptSchema();
    schema1.setAction("summarize");
    schema1.setEntities(List.of("sale"));
    schema1.setParams(Map.of("year", "2024"));
    schema1.generateCacheKey();

    PromptSchema schema2 = new PromptSchema();
    schema2.setAction("summarize");
    schema2.setEntities(List.of("sale"));
    schema2.setParams(Map.of("year", "2023")); // Different value, same key structure
    schema2.generateCacheKey();

    assertEquals(schema1.getCacheKey(), schema2.getCacheKey(),
        "Schemas with same structure (different values) should have same cache key");

    // Store with schema1, lookup with schema2
    cache.store(schema1, testPlan);
    Optional<ExecutionPlan> result = cache.lookup(schema2);
    assertTrue(result.isPresent(), "Should find plan using schema with same structure");
  }
}


