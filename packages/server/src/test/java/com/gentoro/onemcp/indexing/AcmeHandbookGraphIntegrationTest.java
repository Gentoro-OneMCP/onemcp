package com.gentoro.onemcp.indexing;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.HandbookFactory;
import com.gentoro.onemcp.indexing.driver.memory.InMemoryGraphDriver;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.model.Tool;
import java.util.*;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration tests that index the real ACME handbook into an in-memory graph and validate
 * nodes, relations and orphan-doc fallback behaviour.
 */
class AcmeHandbookGraphIntegrationTest {

  private InMemoryGraphDriver driver;
  private HandbookGraphService graphService;

  @BeforeEach
  void setUp() {
    driver = new InMemoryGraphDriver("acme-itest");
  }

  @AfterEach
  void tearDown() {
    if (graphService != null) {
      graphService.close();
    } else if (driver != null) {
      driver.shutdown();
    }
  }

  /**
   * Lightweight OneMcp for tests, allowing us to inject configuration and handbook while
   * keeping construction consistent with production code.
   */
  static class TestOneMcp extends OneMcp {
    private final Configuration cfg;
    private final LlmClient llm;
    private Handbook hb;

    TestOneMcp(Configuration cfg) {
      super(new String[] {});
      this.cfg = cfg;
      this.llm = new DummyLlm();
    }

    void setHandbook(Handbook hb) {
      this.hb = hb;
    }

    @Override
    public Configuration configuration() {
      return cfg;
    }

    @Override
    public Handbook handbook() {
      return hb;
    }

    @Override
    public LlmClient llmClient() {
      return llm;
    }
  }

  /**
   * Deterministic LLM stub used by EntityExtractor during indexing.
   *
   * <p>For these tests we want stable, local "semantic" behaviour without calling a real LLM.
   * The stub inspects the prompt text and tags chunks with ACME entities when they are mentioned
   * in the content:
   *
   * <ul>
   *   <li>If the prompt contains "Category", "Product", "Customer", "Order" or "Region"
   *       (case-insensitive), the corresponding entities are returned as matches.</li>
   *   <li>If none of those tokens appear, an empty match list is returned.</li>
   * </ul>
   */
  static class DummyLlm implements LlmClient {
    @Override
    public String chat(
        java.util.List<Message> messages,
        java.util.List<Tool> tools,
        boolean cacheable,
        InferenceEventListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String generate(
        String message,
        java.util.List<Tool> tools,
        boolean cacheable,
        InferenceEventListener listener) {
      String lower = message != null ? message.toLowerCase() : "";
      java.util.List<String> entities = new java.util.ArrayList<>();
      if (lower.contains("category")) {
        entities.add("Category");
      }
      if (lower.contains("product")) {
        entities.add("Product");
      }
      if (lower.contains("customer")) {
        entities.add("Customer");
      }
      if (lower.contains("order")) {
        entities.add("Order");
      }
      if (lower.contains("region")) {
        entities.add("Region");
      }

      StringBuilder sb = new StringBuilder();
      sb.append("{\n");
      sb.append("  \"chunkId\": \"test\",\n");
      sb.append("  \"matches\": [\n");
      for (int i = 0; i < entities.size(); i++) {
        String e = entities.get(i);
        sb.append("    {\n");
        sb.append("      \"entity\": \"").append(e).append("\",\n");
        sb.append("      \"confidence\": 1.0,\n");
        sb.append("      \"reason\": \"dummy-llm-test\"\n");
        sb.append("    }");
        if (i < entities.size() - 1) {
          sb.append(",");
        }
        sb.append("\n");
      }
      sb.append("  ]\n");
      sb.append("}\n");
      return sb.toString();
    }

    @Override
    public TelemetryScope withTelemetry(TelemetrySink sink) {
      return () -> {};
    }
  }

  private TestOneMcp newAcmeMcp() {
    BaseConfiguration cfg = new BaseConfiguration();
    // Use the built-in ACME handbook from classpath.
    cfg.addProperty("handbook.location", "classpath:acme-handbook");
    cfg.addProperty("indexing.graph.clearOnStartup", true);
    // Enable only structural validation for deterministic, strict checks.
    cfg.addProperty("indexing.graph.validation.structural.enabled", true);
    cfg.addProperty("indexing.graph.validation.semantic.enabled", false);
    cfg.addProperty("indexing.graph.validation.semantic.requireStructuralPass", false);
    // Keep chunking configuration close to defaults; strategy is inferred inside the service.
    cfg.addProperty("indexing.graph.chunking.markdown.windowSizeTokens", 500);
    cfg.addProperty("indexing.graph.chunking.markdown.overlapTokens", 64);

    TestOneMcp mcp = new TestOneMcp(cfg);
    Handbook handbook = HandbookFactory.create(mcp);
    mcp.setHandbook(handbook);
    return mcp;
  }

  @Test
  @DisplayName("ACME handbook graph passes strict structural validation with no errors or warnings")
  void acmeHandbookGraphIsStructurallyValid() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      GraphValidationService validator = new GraphValidationService(mcp, svc);
      GraphValidationService.ValidationReport report = validator.validate();

      assertTrue(
          report.getErrors().isEmpty(),
          () ->
              "Expected no structural validation errors for ACME handbook, but found: "
                  + report.getErrors());
      assertTrue(
          report.getWarnings().isEmpty(),
          () ->
              "Expected no structural validation warnings for ACME handbook, but found: "
                  + report.getWarnings());
    }
  }

  @Test
  @DisplayName("ACME handbook indexing creates DOCUMENT nodes for all markdown documentation files")
  void acmeHandbookCreatesDocumentNodes() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      List<Map<String, Object>> all = driver.queryByContext(List.of());
      assertFalse(all.isEmpty(), "Graph should contain nodes after indexing ACME handbook");

      List<Map<String, Object>> documentNodes =
          all.stream().filter(m -> "DOCUMENT".equals(m.get("nodeType"))).toList();

      assertFalse(documentNodes.isEmpty(), "Expected DOCUMENT nodes for ACME docs");

      // Each DOCUMENT node must have a docPath
      for (Map<String, Object> doc : documentNodes) {
        assertNotNull(doc.get("docPath"), "DOCUMENT node must have docPath");
      }
    }
  }

  @Test
  @DisplayName("ACME handbook indexing creates DOCS_CHUNK nodes for all document chunks")
  void acmeHandbookCreatesChunkNodes() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      List<Map<String, Object>> all = driver.queryByContext(List.of());
      List<Map<String, Object>> chunkNodes =
          all.stream().filter(m -> "DOCS_CHUNK".equals(m.get("nodeType"))).toList();

      assertFalse(chunkNodes.isEmpty(), "Expected DOCS_CHUNK nodes for ACME docs");
    }
  }

  @Test
  @DisplayName("All DOCS_CHUNK nodes have valid parentDocumentKey references to existing DOCUMENT nodes")
  void acmeHandbookChunksHaveValidParentDocumentReferences() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      List<Map<String, Object>> all = driver.queryByContext(List.of());

      List<Map<String, Object>> documentNodes =
          all.stream().filter(m -> "DOCUMENT".equals(m.get("nodeType"))).toList();
      List<Map<String, Object>> chunkNodes =
          all.stream().filter(m -> "DOCS_CHUNK".equals(m.get("nodeType"))).toList();

      // Build set of valid document keys
      Set<String> documentKeys = new HashSet<>();
      for (Map<String, Object> doc : documentNodes) {
        documentKeys.add((String) doc.get("key"));
      }

      // Each DOCS_CHUNK must have a valid parentDocumentKey pointing to an existing DOCUMENT node
      for (Map<String, Object> chunk : chunkNodes) {
        String parentKey = (String) chunk.get("parentDocumentKey");
        assertNotNull(parentKey, "Chunk should have parentDocumentKey: " + chunk.get("key"));
        assertTrue(
            documentKeys.contains(parentKey),
            () ->
                "Chunk parentDocumentKey '"
                    + parentKey
                    + "' should reference an existing DOCUMENT node, but found keys: "
                    + documentKeys);
      }
    }
  }

  @Test
  @DisplayName("Orphaned chunks are retrievable via DOCUMENT entity fallback and semantically correct for plan generation")
  void orphanedChunksRetrievedViaDocumentFallback() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();

      // Start from a clean in-memory graph so we can exercise fallback behaviour in isolation.
      driver.clearAll();

      // Create a DOCUMENT node that declares the "Order" entity.
      GraphNodeRecord document =
          new GraphNodeRecord("doc-1", KnowledgeNodeType.DOCUMENT)
              .setDocPath("docs/orphan-source.md")
              .setEntities(List.of("Order"))
              .setContentFormat("markdown");

      // Insert an "orphan-like" chunk: it has no entities but is linked to the DOCUMENT via
      // parentDocumentKey. The content is semantically relevant to Order entity (mentions sale/order
      // concepts) to ensure it's useful for plan generation even though entity extraction failed.
      String semanticallyRelevantContent =
          "Sales transactions represent individual orders. Each order contains sale.id, sale.amount, "
              + "and sale.date fields. Orders can be queried using the querySalesData operation.";
      GraphNodeRecord orphanChunk =
          new GraphNodeRecord("orphan-chunk", KnowledgeNodeType.DOCS_CHUNK)
              .setDocPath("docs/orphan-source.md")
              .setParentDocumentKey(document.getKey())
              .setEntities(Collections.emptyList())
              .setOperations(Collections.emptyList())
              .setContent(semanticallyRelevantContent)
              .setContentFormat("markdown");

      driver.upsertNodes(List.of(document, orphanChunk));

      // Query by context for the "Order" entity. There are no direct matches on chunks
      // (orphan has no entities), so the InMemoryGraphDriver must:
      //   1) match the DOCUMENT by its entities
      //   2) return its chunks, including the orphan chunk, via fallback.
      List<Map<String, Object>> results =
          svc.retrieveByContext(List.of(new GraphContextTuple("Order", List.of())));

      assertFalse(
          results.isEmpty(), "Expected fallback retrieval to return at least one chunk for Order");

      // Verify the orphan chunk is returned via fallback
      boolean foundOrphan =
          results.stream().anyMatch(m -> "orphan-chunk".equals(m.get("key")));
      assertTrue(
          foundOrphan,
          "Results should include the orphan chunk returned via DOCUMENT entity fallback");

      // Semantic validation: Verify the orphan chunk is usable by ContextDecorator for plan generation
      ContextDecorator decorator = new ContextDecorator(results);
      String generalDocs = decorator.getGeneralDocs();
      
      assertFalse(
          generalDocs.isBlank(),
          "ContextDecorator.getGeneralDocs() should return non-empty content including orphan chunk");
      
      assertTrue(
          generalDocs.contains(semanticallyRelevantContent),
          "ContextDecorator should include orphan chunk content in general docs for plan generation");

      // Semantic relevance validation: Orphan chunk content should be relevant to Order entity
      // even though it wasn't tagged with entities. This ensures plan generation receives useful context.
      String generalDocsLower = generalDocs.toLowerCase();
      boolean hasOrderRelevantConcepts =
          generalDocsLower.contains("sale") || generalDocsLower.contains("order");
      assertTrue(
          hasOrderRelevantConcepts,
          () ->
              "Orphan chunk content should be semantically relevant to Order entity for plan generation. "
                  + "Content should mention sale/order concepts, but was: "
                  + generalDocs.substring(0, Math.min(200, generalDocs.length())));

      // Verify the orphan chunk has the expected structure for ContextDecorator processing
      Map<String, Object> orphanNode =
          results.stream()
              .filter(m -> "orphan-chunk".equals(m.get("key")))
              .findFirst()
              .orElseThrow();
      assertEquals(
          "DOCS_CHUNK",
          orphanNode.get("nodeType"),
          "Orphan chunk should have correct nodeType for ContextDecorator");
      assertNotNull(
          orphanNode.get("content"),
          "Orphan chunk should have content for ContextDecorator.getGeneralDocs()");
      assertEquals(
          document.getKey(),
          orphanNode.get("parentDocumentKey"),
          "Orphan chunk should maintain parentDocumentKey reference");
    }
  }

  @Test
  @DisplayName("Orphaned chunks from real ACME handbook indexing are semantically correct for plan generation")
  void acmeHandbookOrphanedChunksAreSemanticallyCorrect() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      // Find chunks that have no entities (orphaned chunks) but have parentDocumentKey
      List<Map<String, Object>> allNodes = driver.queryByContext(List.of());
      List<Map<String, Object>> orphanedChunks =
          allNodes.stream()
              .filter(m -> "DOCS_CHUNK".equals(m.get("nodeType")))
              .filter(
                  m -> {
                    @SuppressWarnings("unchecked")
                    List<String> entities =
                        (List<String>) m.getOrDefault("entities", Collections.emptyList());
                    return entities == null || entities.isEmpty();
                  })
              .filter(m -> m.get("parentDocumentKey") != null)
              .toList();

      // If there are orphaned chunks, validate they're semantically correct
      if (!orphanedChunks.isEmpty()) {
        // Group orphaned chunks by their parent document
        Map<String, List<Map<String, Object>>> orphansByDocument = new HashMap<>();
        for (Map<String, Object> orphan : orphanedChunks) {
          String parentKey = (String) orphan.get("parentDocumentKey");
          orphansByDocument.computeIfAbsent(parentKey, k -> new ArrayList<>()).add(orphan);
        }

        // For each document with orphaned chunks, verify semantic correctness
        for (Map.Entry<String, List<Map<String, Object>>> entry : orphansByDocument.entrySet()) {
          String documentKey = entry.getKey();
          List<Map<String, Object>> documentOrphans = entry.getValue();

          // Find the parent document to get its entities
          Map<String, Object> parentDocument =
              allNodes.stream()
                  .filter(m -> documentKey.equals(m.get("key")))
                  .findFirst()
                  .orElse(null);

          if (parentDocument != null) {
            @SuppressWarnings("unchecked")
            List<String> documentEntities =
                (List<String>)
                    parentDocument.getOrDefault("entities", Collections.emptyList());

            // Query by context using the document's entities
            if (!documentEntities.isEmpty()) {
              String entity = documentEntities.get(0);
              List<GraphContextTuple> context = List.of(new GraphContextTuple(entity, List.of()));
              List<Map<String, Object>> retrievedResults = svc.retrieveByContext(context);

              // Verify orphaned chunks are retrieved via fallback
              for (Map<String, Object> orphan : documentOrphans) {
                String orphanKey = (String) orphan.get("key");
                boolean orphanRetrieved =
                    retrievedResults.stream()
                        .anyMatch(m -> orphanKey.equals(m.get("key")));
                assertTrue(
                    orphanRetrieved,
                    () ->
                        "Orphaned chunk "
                            + orphanKey
                            + " should be retrievable via document entity fallback for entity "
                            + entity);

                // Verify orphaned chunk is usable by ContextDecorator for plan generation
                ContextDecorator decorator = new ContextDecorator(retrievedResults);
                String generalDocs = decorator.getGeneralDocs();
                String orphanContent = (String) orphan.getOrDefault("content", "");

                assertFalse(
                    orphanContent.isBlank(),
                    () -> "Orphaned chunk " + orphanKey + " should have non-empty content");

                assertTrue(
                    generalDocs.contains(orphanContent),
                    () ->
                        "ContextDecorator.getGeneralDocs() should include orphaned chunk "
                            + orphanKey
                            + " content for plan generation");

                // Semantic relevance: orphan content should be relevant to document's entity
                // (even if entity extraction failed, the content should still be useful)
                String orphanContentLower = orphanContent.toLowerCase();
                String entityLower = entity.toLowerCase();
                boolean hasRelevantContent =
                    orphanContentLower.contains(entityLower)
                        || orphanContentLower.contains("sale")
                        || orphanContentLower.contains("query")
                        || orphanContentLower.contains("data");
                assertTrue(
                    hasRelevantContent,
                    () ->
                        "Orphaned chunk "
                            + orphanKey
                            + " content should be semantically relevant to document entity "
                            + entity
                            + " for plan generation. Content: "
                            + orphanContent.substring(0, Math.min(150, orphanContent.length())));
              }
            }
          }
        }
      }
      // If no orphaned chunks exist, that's also valid - all chunks were successfully tagged
    }
  }

  @Test
  @DisplayName("GraphContextTuple with single entity (Order) returns only nodes whose entities match")
  void orderEntityContextReturnsOnlyMatchingNodes() {
    validateEntityContextMatching("Order", List.of("Retrieve"));
  }

  @Test
  @DisplayName("GraphContextTuple with single entity (Customer) returns only nodes whose entities match")
  void customerEntityContextReturnsOnlyMatchingNodes() {
    validateEntityContextMatching("Customer", List.of("Retrieve"));
  }

  @Test
  @DisplayName("GraphContextTuple with multiple entities (Order and Customer) returns nodes matching any entity")
  void multipleEntityContextReturnsNodesMatchingAnyEntity() {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      // Context: Order and Customer, both with Retrieve operation (ACME Agent.yaml defines
      // Retrieve as the primary operation for these entities).
      List<GraphContextTuple> context =
          List.of(
              new GraphContextTuple("Order", List.of("Retrieve")),
              new GraphContextTuple("Customer", List.of("Retrieve")));

      List<Map<String, Object>> results = svc.retrieveByContext(context);
      assertFalse(
          results.isEmpty(),
          "Expected non-empty results when querying ACME handbook for Order/Customer context");

      // All returned nodes must have entities that intersect the requested entities.
      var requestedEntities = new java.util.HashSet<>(List.of("Order", "Customer"));
      for (Map<String, Object> node : results) {
        @SuppressWarnings("unchecked")
        List<String> entities =
            (List<String>) node.getOrDefault("entities", Collections.emptyList());
        assertFalse(
            entities == null || entities.isEmpty(),
            "Retrieved node should have at least one entity assigned");
        boolean intersects = entities.stream().anyMatch(requestedEntities::contains);
        assertTrue(
            intersects,
            () ->
                "Node entities "
                    + entities
                    + " should intersect requested entities "
                    + requestedEntities);
      }
    }
  }

  /**
   * Validates that GraphContextTuple-based retrieval returns only nodes whose entities match the requested entity.
   */
  private void validateEntityContextMatching(String entity, List<String> operations) {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      List<GraphContextTuple> context = List.of(new GraphContextTuple(entity, operations));
      List<Map<String, Object>> results = svc.retrieveByContext(context);

      assertFalse(
          results.isEmpty(),
          () -> "Expected non-empty results when querying ACME handbook for " + entity + " context");

      // All returned nodes must have entities that include the requested entity
      for (Map<String, Object> node : results) {
        @SuppressWarnings("unchecked")
        List<String> nodeEntities =
            (List<String>) node.getOrDefault("entities", Collections.emptyList());
        assertFalse(
            nodeEntities == null || nodeEntities.isEmpty(),
            () -> "Retrieved node should have at least one entity assigned: " + node.get("key"));
        assertTrue(
            nodeEntities.contains(entity),
            () ->
                "Node entities "
                    + nodeEntities
                    + " should include requested entity "
                    + entity
                    + " for node: "
                    + node.get("key"));
      }
    }
  }

  @Test
  @DisplayName("Category entity: GraphContextTuple retrieval returns complete API docs, I/O schemas and examples with semantic relevance")
  void categoryEntityHasCompleteApiCoverageForPlanGeneration() {
    validateEntityApiCoverage("Category");
  }

  @Test
  @DisplayName("Product entity: GraphContextTuple retrieval returns complete API docs, I/O schemas and examples with semantic relevance")
  void productEntityHasCompleteApiCoverageForPlanGeneration() {
    validateEntityApiCoverage("Product");
  }

  @Test
  @DisplayName("Customer entity: GraphContextTuple retrieval returns complete API docs, I/O schemas and examples with semantic relevance")
  void customerEntityHasCompleteApiCoverageForPlanGeneration() {
    validateEntityApiCoverage("Customer");
  }

  @Test
  @DisplayName("Order entity: GraphContextTuple retrieval returns complete API docs, I/O schemas and examples with semantic relevance")
  void orderEntityHasCompleteApiCoverageForPlanGeneration() {
    validateEntityApiCoverage("Order");
  }

  @Test
  @DisplayName("Region entity: GraphContextTuple retrieval returns complete API docs, I/O schemas and examples with semantic relevance")
  void regionEntityHasCompleteApiCoverageForPlanGeneration() {
    validateEntityApiCoverage("Region");
  }

  /**
   * Validates that a specific entity has complete API coverage for plan generation.
   * This includes checking node types, entity tagging, and semantic content relevance.
   */
  private void validateEntityApiCoverage(String entity) {
    TestOneMcp mcp = newAcmeMcp();

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      this.graphService = svc;
      svc.initialize();
      svc.indexHandbook();

      List<Map<String, Object>> results =
          svc.retrieveByContext(List.of(new GraphContextTuple(entity, List.of("Retrieve"))));

      assertFalse(
          results.isEmpty(),
          () -> "Expected non-empty graph results for entity " + entity + " with Retrieve");

      // Filter only API-related nodes – these are what ContextDecorator feeds into plan_generation
      List<Map<String, Object>> apiDocs =
          results.stream()
              .filter(m -> "API_DOCUMENTATION".equals(m.get("nodeType")))
              .toList();
      List<Map<String, Object>> opDocs =
          results.stream()
              .filter(m -> "API_OPERATION_DOCUMENTATION".equals(m.get("nodeType")))
              .toList();
      List<Map<String, Object>> opInputs =
          results.stream()
              .filter(m -> "API_OPERATION_INPUT".equals(m.get("nodeType")))
              .toList();
      List<Map<String, Object>> opOutputs =
          results.stream()
              .filter(m -> "API_OPERATION_OUTPUT".equals(m.get("nodeType")))
              .toList();
      List<Map<String, Object>> opExamples =
          results.stream()
              .filter(m -> "API_OPERATION_EXAMPLE".equals(m.get("nodeType")))
              .toList();

      // Each entity should see the core Sales Analytics API documentation.
      assertFalse(
          apiDocs.isEmpty(),
          () -> "Expected API_DOCUMENTATION nodes for entity " + entity + " context");

      // At least one operation doc, input schema, output schema, and example should be present
      // for this entity's Retrieve capabilities. Plan generation relies on all of these
      // categories via ContextDecorator.getApiDocs().
      assertFalse(
          opDocs.isEmpty(),
          () ->
              "Expected API_OPERATION_DOCUMENTATION nodes for entity "
                  + entity
                  + " context");
      assertFalse(
          opInputs.isEmpty(),
          () ->
              "Expected API_OPERATION_INPUT nodes (request schema) for entity "
                  + entity
                  + " context");
      assertFalse(
          opOutputs.isEmpty(),
          () ->
              "Expected API_OPERATION_OUTPUT nodes (response schema) for entity "
                  + entity
                  + " context");
      assertFalse(
          opExamples.isEmpty(),
          () ->
              "Expected API_OPERATION_EXAMPLE nodes for entity "
                  + entity
                  + " context");

      // All API-related nodes returned for this entity must have an entities list that
      // includes the entity itself, ensuring ContextDecorator grouping is consistent.
      for (Map<String, Object> node :
          List.of(apiDocs, opDocs, opInputs, opOutputs, opExamples).stream()
              .flatMap(List::stream)
              .toList()) {
        @SuppressWarnings("unchecked")
        List<String> nodeEntities =
            (List<String>) node.getOrDefault("entities", Collections.emptyList());
        assertTrue(
            nodeEntities.contains(entity),
            () ->
                "Expected node "
                    + node.get("key")
                    + " of type "
                    + node.get("nodeType")
                    + " to include entity "
                    + entity
                    + " in its entities list, but found "
                    + nodeEntities);
      }

      // Semantic content validation: verify that retrieved content is actually relevant to the entity
      validateSemanticContentForEntity(entity, apiDocs, opDocs, opInputs, opOutputs, opExamples);
    }
  }

  /**
   * Validates that the content of retrieved nodes is semantically relevant to the queried entity.
   * This ensures that GraphContextTuple-based retrieval returns not just correctly-typed nodes,
   * but nodes whose content actually supports plan generation for that entity.
   */
  private void validateSemanticContentForEntity(
      String entity,
      List<Map<String, Object>> apiDocs,
      List<Map<String, Object>> opDocs,
      List<Map<String, Object>> opInputs,
      List<Map<String, Object>> opOutputs,
      List<Map<String, Object>> opExamples) {

    // Define expected semantic markers for each entity based on ACME schema
    // These are used to validate that operation docs, I/O schemas, and examples are entity-relevant
    Map<String, List<String>> entitySemanticMarkers = Map.of(
        "Order", List.of("sale", "order", "transaction", "purchase", "sale.id", "sale.amount", "sale.date"),
        "Product", List.of("product", "item", "product.id", "product.name", "product.category", "product.price"),
        "Customer", List.of("customer", "buyer", "client", "customer.id", "customer.name", "customer.email"),
        "Category", List.of("category", "group", "product.category"),
        "Region", List.of("region", "geographic", "location", "customer.state", "customer.city", "region.name"));

    List<String> expectedMarkers = entitySemanticMarkers.getOrDefault(entity, List.of(entity.toLowerCase()));

    // API_DOCUMENTATION is general and covers all entities, so we only validate it's non-empty
    // and mentions general API concepts (not entity-specific)
    for (Map<String, Object> apiDoc : apiDocs) {
      String content = (String) apiDoc.getOrDefault("content", "");
      assertFalse(
          content.isBlank(),
          () -> "API_DOCUMENTATION node should have non-empty content for entity " + entity);
      
      // API docs are general, so just check they mention general API concepts
      String contentLower = content.toLowerCase();
      boolean hasGeneralApiConcepts = contentLower.contains("api") 
          || contentLower.contains("query") 
          || contentLower.contains("data")
          || contentLower.contains("analytics");
      assertTrue(
          hasGeneralApiConcepts,
          () -> String.format(
              "API_DOCUMENTATION content should mention general API concepts, but content was: %s",
              content.substring(0, Math.min(200, content.length()))));
    }

    // Validate API_OPERATION_DOCUMENTATION content is relevant
    // Since all ACME entities use the same querySalesData operation, operation docs are general,
    // but we validate they mention the operation and query-related concepts
    for (Map<String, Object> opDoc : opDocs) {
      String content = (String) opDoc.getOrDefault("content", "");
      assertFalse(
          content.isBlank(),
          () -> "API_OPERATION_DOCUMENTATION node should have non-empty content for entity " + entity);
      
      String contentLower = content.toLowerCase();
      // Operation docs should mention the operation (querySalesData) or query-related concepts
      boolean hasOperationInfo = contentLower.contains("query") 
          || contentLower.contains("operation")
          || contentLower.contains("querysalesdata")
          || contentLower.contains("post");
      assertTrue(
          hasOperationInfo,
          () -> String.format(
              "API_OPERATION_DOCUMENTATION content should mention operation/query concepts, but content was: %s",
              content.substring(0, Math.min(200, content.length()))));
    }

    // Validate API_OPERATION_INPUT schema structure
    // Since all entities use the same querySalesData operation, input schemas are generic query structures
    // We validate they contain query-related concepts (filter, fields, etc.) where entity-specific fields would be used
    for (Map<String, Object> opInput : opInputs) {
      String schemaContent = (String) opInput.getOrDefault("content", "");
      assertFalse(
          schemaContent.isBlank(),
          () -> "API_OPERATION_INPUT node should have non-empty JSON schema for entity " + entity);
      
      String schemaLower = schemaContent.toLowerCase();
      // Input schemas should mention filter/fields concepts where entity-specific fields are used
      boolean hasQueryStructure = schemaLower.contains("filter") 
          || schemaLower.contains("fields")
          || schemaLower.contains("query")
          || schemaLower.contains("properties");
      assertTrue(
          hasQueryStructure,
          () -> String.format(
              "API_OPERATION_INPUT schema should contain query structure concepts (filter/fields), but schema was: %s",
              schemaContent.substring(0, Math.min(300, schemaContent.length()))));
    }

    // Validate API_OPERATION_OUTPUT schema structure
    // Output schemas are generic response structures, we validate they mention response/data concepts
    for (Map<String, Object> opOutput : opOutputs) {
      String schemaContent = (String) opOutput.getOrDefault("content", "");
      assertFalse(
          schemaContent.isBlank(),
          () -> "API_OPERATION_OUTPUT node should have non-empty JSON schema for entity " + entity);
      
      String schemaLower = schemaContent.toLowerCase();
      // Output schemas should mention response/data concepts
      boolean hasResponseStructure = schemaLower.contains("response") 
          || schemaLower.contains("data")
          || schemaLower.contains("properties")
          || schemaLower.contains("object");
      assertTrue(
          hasResponseStructure,
          () -> String.format(
              "API_OPERATION_OUTPUT schema should contain response structure concepts, but schema was: %s",
              schemaContent.substring(0, Math.min(300, schemaContent.length()))));
    }

    // Validate API_OPERATION_EXAMPLE content shows valid query examples
    // Since all entities use the same querySalesData operation, examples are shared and show query structure
    // We validate that examples demonstrate query usage (filter/fields/aggregates) which is what plan generation needs
    for (Map<String, Object> opExample : opExamples) {
      String content = (String) opExample.getOrDefault("content", "");
      assertFalse(
          content.isBlank(),
          () -> "API_OPERATION_EXAMPLE node should have non-empty example content for entity " + entity);
      
      String contentLower = content.toLowerCase();
      // Examples should show query structure (filter, fields, aggregates) or JSON structure
      boolean hasQueryStructure = contentLower.contains("filter") 
          || contentLower.contains("fields")
          || contentLower.contains("aggregate")
          || contentLower.contains("json")
          || contentLower.contains("{");
      assertTrue(
          hasQueryStructure,
          () -> String.format(
              "API_OPERATION_EXAMPLE content should demonstrate query structure (filter/fields/aggregates), but example was: %s",
              content.substring(0, Math.min(300, content.length()))));
    }
    
    // Additionally, validate that at least one example across all examples for this entity
    // mentions entity-relevant field patterns (this ensures semantic relevance for plan generation)
    if (!opExamples.isEmpty()) {
      boolean hasAnyEntityRelevance = opExamples.stream()
          .anyMatch(ex -> {
            String exContent = ((String) ex.getOrDefault("content", "")).toLowerCase();
            return expectedMarkers.stream()
                .anyMatch(marker -> {
                  String lowerMarker = marker.toLowerCase();
                  // Check for field patterns like "customer.id", "sale.amount", "product.category"
                  return exContent.contains(lowerMarker + ".") 
                      || (lowerMarker.length() > 4 && exContent.contains(lowerMarker));
                });
          });
      assertTrue(
          hasAnyEntityRelevance,
          () -> String.format(
              "At least one API_OPERATION_EXAMPLE for entity %s should use entity-relevant field patterns (%s). Examples checked: %d",
              entity, expectedMarkers, opExamples.size()));
    }
  }
}


