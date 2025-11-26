package com.gentoro.onemcp.indexing;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GraphNodeRecordTest {

  @Test
  @DisplayName("toMap/fromMap round-trip preserves fields and list contents")
  void testRoundTrip() {
    GraphNodeRecord r =
        new GraphNodeRecord("k1", KnowledgeNodeType.API_OPERATION_EXAMPLE)
            .setApiSlug("orders-api")
            .setOperationId("getOrder")
            .setContent("markdown body")
            .setContentFormat("markdown")
            .setDocPath("/docs/a.md")
            .setTitle("Example A")
            .setSummary("summary")
            .setEntities(List.of("Order", "User"))
            .setOperations(List.of("Retrieve", "List"));

    Map<String, Object> map = r.toMap();
    GraphNodeRecord back = GraphNodeRecord.fromMap(map);

    assertEquals(r.getKey(), back.getKey());
    assertEquals(r.getNodeType(), back.getNodeType());
    assertEquals(r.getApiSlug(), back.getApiSlug());
    assertEquals(r.getOperationId(), back.getOperationId());
    assertEquals(r.getContent(), back.getContent());
    assertEquals(r.getContentFormat(), back.getContentFormat());
    assertEquals(r.getDocPath(), back.getDocPath());
    assertEquals(r.getTitle(), back.getTitle());
    assertEquals(r.getSummary(), back.getSummary());
    assertIterableEquals(r.getEntities(), back.getEntities());
    assertIterableEquals(r.getOperations(), back.getOperations());
  }

  @Test
  @DisplayName("Null/empty handling: lists default to empty and maps contain them")
  void testNullEmptyHandling() {
    GraphNodeRecord r =
        new GraphNodeRecord("k2", KnowledgeNodeType.DOCS_CHUNK)
            .setEntities(null)
            .setOperations(null)
            .setContent("c");
    Map<String, Object> map = r.toMap();
    assertTrue(map.containsKey("entities"));
    assertTrue(map.containsKey("operations"));
    assertTrue(((List<?>) map.get("entities")).isEmpty());
    assertTrue(((List<?>) map.get("operations")).isEmpty());

    GraphNodeRecord back = GraphNodeRecord.fromMap(map);
    assertNotNull(back.getEntities());
    assertNotNull(back.getOperations());
    assertTrue(back.getEntities().isEmpty());
    assertTrue(back.getOperations().isEmpty());
  }
}
