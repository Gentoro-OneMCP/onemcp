package com.gentoro.onemcp.indexing;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Agent;
import com.gentoro.onemcp.indexing.driver.memory.InMemoryGraphDriver;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.model.Tool;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HandbookGraphServiceTest {

  private Path tmp;
  private InMemoryGraphDriver driver;

  @BeforeEach
  void setUp() throws Exception {
    tmp = Files.createTempDirectory("hb_svc_");
    driver = new InMemoryGraphDriver("hb-test");
  }

  @AfterEach
  void tearDown() throws Exception {
    driver.shutdown();
  }

  // Lightweight test MCP that allows injecting configuration and handbook without heavy
  // initialization
  static class TestMcp extends OneMcp {
    private final Configuration cfg;
    private Handbook hb;
    private final LlmClient llm;

    TestMcp(Configuration cfg, Handbook hb) {
      super(new String[] {});
      this.cfg = cfg;
      this.hb = hb;
      this.llm = new DummyLlm();
    }

    @Override
    public Configuration configuration() {
      return cfg;
    }

    @Override
    public Handbook handbook() {
      return hb;
    }

    public void setHandbook(Handbook hb) {
      this.hb = hb;
    }

    @Override
    public LlmClient llmClient() {
      return llm;
    }
  }

  // Minimal LLM that returns a deterministic JSON match for entity "Order"
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
      return "{\n  \"chunkId\": \"test\",\n  \"matches\": [{\n    \"entity\": \"Order\", \n    \"confidence\": 1.0, \n    \"reason\": \"test\"\n  }]\n}";
    }

    @Override
    public TelemetryScope withTelemetry(TelemetrySink sink) {
      return () -> {};
    }
  }

  private Handbook newHandbookWithDocs(Map<String, String> docs, OneMcp mcp) {
    return new Handbook() {
      private final Agent agent = new Agent();

      @Override
      public Path location() {
        return tmp;
      }

      @Override
      public Agent agent() {
        return agent;
      }

      @Override
      public java.util.Optional<com.gentoro.onemcp.handbook.model.agent.Api> optionalApi(
          String slug) {
        return java.util.Optional.empty();
      }

      @Override
      public com.gentoro.onemcp.handbook.model.agent.Api api(String slug) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String, com.gentoro.onemcp.handbook.model.agent.Api> apis() {
        return Map.of();
      }

      @Override
      public Map<String, com.gentoro.onemcp.handbook.model.regression.RegressionSuite>
          regressionSuites() {
        return Map.of();
      }

      @Override
      public java.util.Optional<com.gentoro.onemcp.handbook.model.regression.RegressionSuite>
          optionalRegressionSuite(String relativePath) {
        return java.util.Optional.empty();
      }

      @Override
      public com.gentoro.onemcp.handbook.model.regression.RegressionSuite regressionSuite(
          String relativePath) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String, String> documentation() {
        return docs;
      }

      @Override
      public java.util.Optional<String> optionalDocumentation(String relativePath) {
        return java.util.Optional.ofNullable(docs.get(relativePath));
      }

      @Override
      public String documentation(String relativePath) {
        return docs.get(relativePath);
      }

      @Override
      public OneMcp oneMcp() {
        return mcp;
      }

      @Override
      public String name() {
        return "hb";
      }
    };
  }

  @Test
  @DisplayName("indexHandbook indexes markdown docs into DOCS_CHUNK nodes with configured strategy")
  void indexHandbookDocsParagraphStrategy() throws Exception {
    // Prepare a real file under the handbook location so service reads from FS
    Path docsDir = Files.createDirectories(tmp.resolve("docs"));
    Path a = docsDir.resolve("a.md");
    Files.writeString(
        a, """
        ---
        entities: [Order]
        ---
        Para1\n\nPara2\n\n""");

    Map<String, String> docs = new HashMap<>();
    docs.put(tmp.relativize(a).toString(), Files.readString(a));

    BaseConfiguration cfg = new BaseConfiguration();
    cfg.addProperty("indexing.graph.clearOnStartup", true);
    cfg.addProperty("indexing.graph.chunking.markdown.strategy", "paragraph");
    // Use a tiny window to ensure separate chunks per paragraph in PARAGRAPH strategy
    cfg.addProperty("indexing.graph.chunking.markdown.windowSizeTokens", 1);
    cfg.addProperty("indexing.graph.chunking.markdown.overlapTokens", 0);

    // Create a lightweight TestMcp and Handbook
    OneMcp mcp = new TestMcp(cfg, null);
    Handbook hb = newHandbookWithDocs(docs, mcp);
    ((TestMcp) mcp).setHandbook(hb);

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      svc.initialize();
      svc.indexHandbook();

      List<Map<String, Object>> all = driver.queryByContext(List.of());
      assertFalse(all.isEmpty());
      long docsCount = all.stream().filter(m -> "DOCS_CHUNK".equals(m.get("nodeType"))).count();
      assertTrue(docsCount >= 1, "At least one chunk expected to be indexed");
    }
  }

  @Test
  @DisplayName("retrieveByContext delegates to driver and filters by entities/ops")
  void retrieveByContext() throws Exception {
    Path docsDir = Files.createDirectories(tmp.resolve("docs"));
    Path a = docsDir.resolve("b.md");
    Files.writeString(
        a,
        """
        ---
        entities:
          - name: Order
            operations: [Retrieve]
        ---
        Some text about orders.
        """);

    Map<String, String> docs = new HashMap<>();
    docs.put(tmp.relativize(a).toString(), Files.readString(a));

    BaseConfiguration cfg = new BaseConfiguration();
    cfg.addProperty("indexing.graph.clearOnStartup", true);
    cfg.addProperty("indexing.graph.chunking.markdown.strategy", "heading");
    cfg.addProperty("indexing.graph.chunking.markdown.windowSizeTokens", 100);

    OneMcp mcp = new TestMcp(cfg, null);
    Handbook hb = newHandbookWithDocs(docs, mcp);
    ((TestMcp) mcp).setHandbook(hb);

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      svc.indexHandbook();

      List<Map<String, Object>> forOrder =
          svc.retrieveByContext(List.of(new GraphContextTuple("Order", List.of("Retrieve"))));
      assertFalse(forOrder.isEmpty());
      // Mismatch entity should return empty
      List<Map<String, Object>> forUser =
          svc.retrieveByContext(List.of(new GraphContextTuple("User", List.of("Retrieve"))));
      assertTrue(forUser.isEmpty());
    }
  }

  @Test
  @DisplayName("indexHandbook creates DOCUMENT nodes and links chunks via parentDocumentKey")
  void documentHierarchyCreation() throws Exception {
    Path docsDir = Files.createDirectories(tmp.resolve("docs"));
    Path doc1 = docsDir.resolve("guide1.md");
    Path doc2 = docsDir.resolve("guide2.md");

    Files.writeString(
        doc1,
        """
        # Guide 1
        This is about Order entities.
        """);

    Files.writeString(
        doc2,
        """
        # Guide 2
        This is about User entities.
        """);

    Map<String, String> docs = new HashMap<>();
    docs.put(tmp.relativize(doc1).toString(), Files.readString(doc1));
    docs.put(tmp.relativize(doc2).toString(), Files.readString(doc2));

    BaseConfiguration cfg = new BaseConfiguration();
    cfg.addProperty("indexing.graph.clearOnStartup", true);
    cfg.addProperty("indexing.graph.chunking.markdown.strategy", "paragraph");
    cfg.addProperty("indexing.graph.chunking.markdown.windowSizeTokens", 500);

    OneMcp mcp = new TestMcp(cfg, null);
    Handbook hb = newHandbookWithDocs(docs, mcp);
    ((TestMcp) mcp).setHandbook(hb);

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      svc.initialize();
      svc.indexHandbook();

      List<Map<String, Object>> all = driver.queryByContext(List.of());

      // Verify DOCUMENT nodes are created
      List<Map<String, Object>> documentNodes =
          all.stream()
              .filter(m -> "DOCUMENT".equals(m.get("nodeType")))
              .toList();
      assertEquals(2, documentNodes.size(), "Should create one DOCUMENT node per file");

      // Verify chunks have parentDocumentKey
      List<Map<String, Object>> chunkNodes =
          all.stream()
              .filter(m -> "DOCS_CHUNK".equals(m.get("nodeType")))
              .toList();
      assertFalse(chunkNodes.isEmpty(), "Should have chunk nodes");

      for (Map<String, Object> chunk : chunkNodes) {
        String parentDocKey = (String) chunk.get("parentDocumentKey");
        assertNotNull(parentDocKey, "Chunk should have parentDocumentKey: " + chunk.get("key"));
        assertTrue(
            parentDocKey.startsWith("document|"),
            "parentDocumentKey should start with 'document|': " + parentDocKey);

        // Verify parent document exists
        boolean parentExists =
            documentNodes.stream().anyMatch(d -> parentDocKey.equals(d.get("key")));
        assertTrue(
            parentExists,
            "Parent document should exist for chunk: " + chunk.get("key") + " -> " + parentDocKey);
      }

      // Verify document nodes have docPath
      for (Map<String, Object> doc : documentNodes) {
        String docPath = (String) doc.get("docPath");
        assertNotNull(docPath, "Document should have docPath");
        assertTrue(
            docPath.contains("guide1.md") || docPath.contains("guide2.md"),
            "Document path should reference source file");
      }
    }
  }

  @Test
  @DisplayName("Fallback retrieval works for orphaned chunks via document hierarchy")
  void fallbackRetrievalViaDocument() throws Exception {
    Path docsDir = Files.createDirectories(tmp.resolve("docs"));
    Path doc = docsDir.resolve("orphaned.md");

    // Create a document with content that may not have direct entity matches
    // The DummyLlm will return "Order" for entity extraction, but we'll test fallback
    Files.writeString(
        doc,
        """
        # Orphaned Content
        This content may not have direct entity matches in some chunks.
        But the document should have entities from the union of chunks.
        """);

    Map<String, String> docs = new HashMap<>();
    docs.put(tmp.relativize(doc).toString(), Files.readString(doc));

    BaseConfiguration cfg = new BaseConfiguration();
    cfg.addProperty("indexing.graph.clearOnStartup", true);
    cfg.addProperty("indexing.graph.chunking.markdown.strategy", "paragraph");
    cfg.addProperty("indexing.graph.chunking.markdown.windowSizeTokens", 100);

    OneMcp mcp = new TestMcp(cfg, null);
    Handbook hb = newHandbookWithDocs(docs, mcp);
    ((TestMcp) mcp).setHandbook(hb);

    try (HandbookGraphService svc = new HandbookGraphService(mcp, driver)) {
      svc.initialize();
      svc.indexHandbook();

      // Query for Order - should find chunks via document fallback if direct match fails
      List<Map<String, Object>> results =
          svc.retrieveByContext(List.of(new GraphContextTuple("Order", List.of())));
      // Should return at least one result (either direct match or via document fallback)
      assertFalse(results.isEmpty(), "Should retrieve chunks via direct match or document fallback");
    }
  }
}
