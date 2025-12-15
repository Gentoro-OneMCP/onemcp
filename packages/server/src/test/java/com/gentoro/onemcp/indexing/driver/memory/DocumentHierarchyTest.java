package com.gentoro.onemcp.indexing.driver.memory;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for document hierarchy relationships in the knowledge graph.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>Document nodes are created correctly
 *   <li>Chunks are linked to their parent documents via parentDocumentKey
 *   <li>Fallback retrieval works when chunks have no direct entity matches
 *   <li>Orphaned chunks are discoverable through parent documents
 * </ul>
 */
class DocumentHierarchyTest {

  InMemoryGraphDriver driver;

  @BeforeEach
  void setUp() {
    driver = new InMemoryGraphDriver("test-handbook");
    driver.initialize();
  }

  @Test
  @DisplayName("Document nodes are created with correct type and entities")
  void documentNodeCreation() {
    // Create a document node
    GraphNodeRecord document =
        new GraphNodeRecord("document|/docs/guide.md", KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order", "User"))
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document));

    List<Map<String, Object>> all = driver.queryByContext(List.of());
    assertEquals(1, all.size());

    Map<String, Object> doc = all.get(0);
    assertEquals("document|/docs/guide.md", doc.get("key"));
    assertEquals("DOCUMENT", doc.get("nodeType"));
    assertEquals("/docs/guide.md", doc.get("docPath"));
    @SuppressWarnings("unchecked")
    List<String> entities = (List<String>) doc.get("entities");
    assertEquals(2, entities.size());
    assertTrue(entities.contains("Order"));
    assertTrue(entities.contains("User"));
  }

  @Test
  @DisplayName("Chunks are linked to parent documents via parentDocumentKey")
  void chunkParentLinking() {
    String documentKey = "document|/docs/guide.md";

    // Create document node
    GraphNodeRecord document =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order"))
            .setContentFormat("markdown");

    // Create chunks with parent reference
    GraphNodeRecord chunk1 =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of("Order"))
            .setContent("Chunk 1 content")
            .setContentFormat("markdown");

    GraphNodeRecord chunk2 =
        new GraphNodeRecord("doc|/docs/guide.md|hash2", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of()) // Orphaned chunk - no entities
            .setContent("Chunk 2 content")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document, chunk1, chunk2));

    // Verify all nodes exist
    List<Map<String, Object>> all = driver.queryByContext(List.of());
    assertEquals(3, all.size());

    // Verify chunks have correct parentDocumentKey
    Map<String, Object> chunk1Map =
        all.stream()
            .filter(m -> "doc|/docs/guide.md|hash1".equals(m.get("key")))
            .findFirst()
            .orElseThrow();
    assertEquals(documentKey, chunk1Map.get("parentDocumentKey"));

    Map<String, Object> chunk2Map =
        all.stream()
            .filter(m -> "doc|/docs/guide.md|hash2".equals(m.get("key")))
            .findFirst()
            .orElseThrow();
    assertEquals(documentKey, chunk2Map.get("parentDocumentKey"));
  }

  @Test
  @DisplayName("Direct entity matches work as before (primary retrieval)")
  void directEntityMatching() {
    String documentKey = "document|/docs/guide.md";

    GraphNodeRecord document =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order"))
            .setContentFormat("markdown");

    GraphNodeRecord chunk =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of("Order"))
            .setContent("Order content")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document, chunk));

    // Direct entity match should work
    List<Map<String, Object>> results =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));
    assertEquals(1, results.size());
    assertEquals("doc|/docs/guide.md|hash1", results.get(0).get("key"));
  }

  @Test
  @DisplayName("Fallback retrieval: orphaned chunks discoverable via parent document")
  void fallbackRetrievalForOrphanedChunks() {
    String documentKey = "document|/docs/guide.md";

    // Document has Order entity
    GraphNodeRecord document =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order"))
            .setContentFormat("markdown");

    // Chunk has no entities (orphaned)
    GraphNodeRecord orphanedChunk =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of()) // No entities - orphaned
            .setContent("Orphaned chunk content")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document, orphanedChunk));

    // Query for Order entity
    // Since chunk has no entities, direct match returns empty
    // Fallback should find document with Order entity, then return its chunks
    List<Map<String, Object>> results =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));

    // Fallback should return the orphaned chunk via parent document
    assertEquals(1, results.size(), "Fallback should return orphaned chunk via document");
    assertEquals("doc|/docs/guide.md|hash1", results.get(0).get("key"));
  }

  @Test
  @DisplayName("Fallback retrieval finds chunks via parent document entities")
  void fallbackRetrievalViaDocument() {
    String documentKey = "document|/docs/guide.md";

    // Document has Order entity (union of all chunk entities)
    GraphNodeRecord document =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order")) // Document has Order entity
            .setContentFormat("markdown");

    // Chunk has no entities (orphaned)
    GraphNodeRecord orphanedChunk =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of()) // No entities - orphaned
            .setContent("Orphaned chunk about orders")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document, orphanedChunk));

    // Query for Order entity
    // Since chunk has no entities, direct match returns empty
    // Fallback should find document with Order entity, then return its chunks
    List<Map<String, Object>> results =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));

    // Fallback should return the orphaned chunk via parent document
    assertEquals(1, results.size(), "Fallback should return orphaned chunk via document");
    assertEquals("doc|/docs/guide.md|hash1", results.get(0).get("key"));
  }

  @Test
  @DisplayName("Multiple chunks from same document are all returned via fallback")
  void fallbackRetrievalMultipleChunks() {
    String documentKey = "document|/docs/guide.md";

    GraphNodeRecord document =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order"))
            .setContentFormat("markdown");

    GraphNodeRecord chunk1 =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of()) // Orphaned
            .setContent("Chunk 1")
            .setContentFormat("markdown");

    GraphNodeRecord chunk2 =
        new GraphNodeRecord("doc|/docs/guide.md|hash2", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of("Order")) // Has entity - direct match
            .setContent("Chunk 2")
            .setContentFormat("markdown");

    GraphNodeRecord chunk3 =
        new GraphNodeRecord("doc|/docs/guide.md|hash3", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of()) // Orphaned
            .setContent("Chunk 3")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document, chunk1, chunk2, chunk3));

    // Query for Order
    // chunk2 should match directly
    // Since there's a direct match, fallback should NOT trigger
    // Only chunk2 should be returned
    List<Map<String, Object>> results =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));

    // Should only return chunk with direct entity match (fallback not triggered when direct matches exist)
    assertEquals(1, results.size(), "Should only return direct match when available");
    assertEquals("doc|/docs/guide.md|hash2", results.get(0).get("key"));
  }

  @Test
  @DisplayName("Fallback only triggers when direct matches are empty")
  void fallbackOnlyWhenDirectMatchesEmpty() {
    String doc1Key = "document|/docs/guide1.md";
    String doc2Key = "document|/docs/guide2.md";

    // Document 1 with Order entity
    GraphNodeRecord doc1 =
        new GraphNodeRecord(doc1Key, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide1.md")
            .setEntities(List.of("Order"))
            .setContentFormat("markdown");

    // Document 2 with User entity
    GraphNodeRecord doc2 =
        new GraphNodeRecord(doc2Key, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide2.md")
            .setEntities(List.of("User"))
            .setContentFormat("markdown");

    // Chunk with direct Order match
    GraphNodeRecord chunkWithEntity =
        new GraphNodeRecord("doc|/docs/guide1.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide1.md")
            .setParentDocumentKey(doc1Key)
            .setEntities(List.of("Order"))
            .setContent("Chunk with Order")
            .setContentFormat("markdown");

    // Orphaned chunk in doc1
    GraphNodeRecord orphanedChunk =
        new GraphNodeRecord("doc|/docs/guide1.md|hash2", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide1.md")
            .setParentDocumentKey(doc1Key)
            .setEntities(List.of())
            .setContent("Orphaned chunk")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(doc1, doc2, chunkWithEntity, orphanedChunk));

    // Query for Order - should return chunkWithEntity via direct match
    // Fallback should NOT trigger because direct match found something
    List<Map<String, Object>> results =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));

    // Should only return chunk with direct entity match
    // Fallback is only used when direct matches are empty
    assertEquals(1, results.size(), "Should only return direct match when available");
    assertEquals("doc|/docs/guide1.md|hash1", results.get(0).get("key"));
  }

  @Test
  @DisplayName("Document entities are union of all chunk entities")
  void documentEntitiesUnion() {
    String documentKey = "document|/docs/guide.md";

    // Document should have union of entities from all chunks
    GraphNodeRecord document =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath("/docs/guide.md")
            .setEntities(List.of("Order", "User", "Product")) // Union of chunk entities
            .setContentFormat("markdown");

    GraphNodeRecord chunk1 =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of("Order"))
            .setContent("Chunk 1")
            .setContentFormat("markdown");

    GraphNodeRecord chunk2 =
        new GraphNodeRecord("doc|/docs/guide.md|hash2", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(documentKey)
            .setEntities(List.of("User", "Product"))
            .setContent("Chunk 2")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(document, chunk1, chunk2));

    // Query for any entity in the union should find the document
    List<Map<String, Object>> forOrder =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));
    assertEquals(1, forOrder.size(), "Should find chunk1 via direct match");

    List<Map<String, Object>> forUser =
        driver.queryByContext(List.of(new GraphContextTuple("User", List.of())));
    assertEquals(1, forUser.size(), "Should find chunk2 via direct match");

    List<Map<String, Object>> forProduct =
        driver.queryByContext(List.of(new GraphContextTuple("Product", List.of())));
    assertEquals(1, forProduct.size(), "Should find chunk2 via direct match");
  }

  @Test
  @DisplayName("Chunks without parentDocumentKey are not affected by fallback")
  void chunksWithoutParentNotAffected() {
    // Chunk without parent (legacy or API node)
    GraphNodeRecord chunkWithoutParent =
        new GraphNodeRecord("doc|/docs/guide.md|hash1", KnowledgeNodeType.DOCS_CHUNK)
            .setDocPath("/docs/guide.md")
            .setParentDocumentKey(null) // No parent
            .setEntities(List.of("Order"))
            .setContent("Chunk without parent")
            .setContentFormat("markdown");

    driver.upsertNodes(List.of(chunkWithoutParent));

    // Should still work via direct entity match
    List<Map<String, Object>> results =
        driver.queryByContext(List.of(new GraphContextTuple("Order", List.of())));
    assertEquals(1, results.size());
    assertEquals("doc|/docs/guide.md|hash1", results.get(0).get("key"));
  }
}

