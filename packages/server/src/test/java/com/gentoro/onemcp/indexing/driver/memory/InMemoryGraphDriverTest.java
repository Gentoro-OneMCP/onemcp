package com.gentoro.onemcp.indexing.driver.memory;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InMemoryGraphDriverTest {

  InMemoryGraphDriver driver;

  @BeforeEach
  void setUp() {
    driver = new InMemoryGraphDriver("test-handbook");
    assertFalse(driver.isInitialized());
    driver.initialize();
    assertTrue(driver.isInitialized());
  }

  @Test
  @DisplayName("Lifecycle: clearAll and shutdown reset store and flags")
  void lifecycle() {
    driver.upsertNodes(
        List.of(
            new GraphNodeRecord("k", KnowledgeNodeType.DOCS_CHUNK)
                .setEntities(List.of("Order"))
                .setContent("a")
                .setContentFormat("markdown")));
    assertFalse(driver.queryByContext(List.of()).isEmpty());

    driver.clearAll();
    assertTrue(driver.queryByContext(List.of()).isEmpty());

    driver.shutdown();
    assertFalse(driver.isInitialized());
    assertTrue(driver.queryByContext(List.of()).isEmpty());
  }

  @Test
  @DisplayName("upsertNodes is idempotent by key and deleteNodesByKeys removes entries")
  void upsertAndDelete() {
    GraphNodeRecord n1 =
        new GraphNodeRecord("k1", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(List.of("Order"))
            .setContent("v1")
            .setContentFormat("markdown");
    GraphNodeRecord n1b =
        new GraphNodeRecord("k1", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(List.of("Order"))
            .setContent("v2")
            .setContentFormat("markdown");
    GraphNodeRecord n2 =
        new GraphNodeRecord("k2", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(List.of("User"))
            .setContent("u")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(n1, n2));
    List<Map<String, Object>> all = driver.queryByContext(List.of());
    assertEquals(2, all.size());

    driver.upsertNodes(List.of(n1b));
    List<Map<String, Object>> after = driver.queryByContext(List.of());
    assertEquals(2, after.size());
    Map<String, Object> updated =
        after.stream().filter(m -> m.get("key").equals("k1")).findFirst().orElseThrow();
    assertEquals("v2", updated.get("content"));

    driver.deleteNodesByKeys(List.of("k1"));
    List<Map<String, Object>> afterDelete = driver.queryByContext(List.of());
    assertEquals(1, afterDelete.size());
    assertEquals("k2", afterDelete.getFirst().get("key"));
  }

  @Test
  @DisplayName("queryByContext: entity-only nodes and entity+operation matching semantics")
  void queryByContextSemantics() {
    GraphNodeRecord entOnly =
        new GraphNodeRecord("e1", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(List.of("Order"))
            .setContent("entOnly")
            .setContentFormat("markdown");
    GraphNodeRecord withOps =
        new GraphNodeRecord("e2", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(List.of("Order"))
            .setOperations(List.of("Retrieve", "Create"))
            .setContent("ops")
            .setContentFormat("markdown");
    GraphNodeRecord other =
        new GraphNodeRecord("e3", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(List.of("User"))
            .setOperations(List.of("Retrieve"))
            .setContent("other")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(entOnly, withOps, other));

    // Empty context returns all
    assertEquals(3, driver.queryByContext(List.of()).size());

    // Entity only context: returns both entity-only and entity+ops for that entity
    List<Map<String, Object>> forOrder =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));
    assertEquals(2, forOrder.size());

    // Entity+operation context: must match operation intersection
    List<Map<String, Object>> forRetrieve =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of("Retrieve"))));
    assertEquals(
        2, forRetrieve.size(), "entity-only node should be included, plus matching op node");

    List<Map<String, Object>> forCreate =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of("Create"))));
    assertEquals(2, forCreate.size());

    List<Map<String, Object>> forUpdate =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of("Update"))));
    assertEquals(1, forUpdate.size(), "only entity-only should match when op does not intersect");

    // Multiple tuples behave as OR across tuples
    List<Map<String, Object>> forOrderOrUser =
        driver.queryByContext(
            List.of(
                new GraphContextTuple("Order", List.of("Retrieve")),
                new GraphContextTuple("User", List.of())));
    assertEquals(3, forOrderOrUser.size());
  }
}
