package com.gentoro.onemcp.indexing;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Agent;
import com.gentoro.onemcp.indexing.driver.memory.InMemoryGraphDriver;
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

    TestMcp(Configuration cfg, Handbook hb) {
      super(new String[] {});
      this.cfg = cfg;
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

    public void setHandbook(Handbook hb) {
      this.hb = hb;
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
    cfg.addProperty("graph.indexing.clearOnStartup", true);
    cfg.addProperty("graph.indexing.chunking.markdown.strategy", "paragraph");
    // Use a tiny window to ensure separate chunks per paragraph in PARAGRAPH strategy
    cfg.addProperty("graph.indexing.chunking.markdown.windowSizeTokens", 1);
    cfg.addProperty("graph.indexing.chunking.markdown.overlapTokens", 0);

    // Create a lightweight TestMcp and Handbook
    OneMcp mcp = new TestMcp(cfg, null);
    Handbook hb = newHandbookWithDocs(docs, mcp);
    ((TestMcp) mcp).setHandbook(hb);

    HandbookGraphService svc = new HandbookGraphService(mcp, driver);
    svc.initialize();
    svc.indexHandbook();

    List<Map<String, Object>> all = driver.queryByContext(List.of());
    assertFalse(all.isEmpty());
    long docsCount = all.stream().filter(m -> "DOCS_CHUNK".equals(m.get("nodeType"))).count();
    assertTrue(docsCount >= 2, "At least two chunks expected for two paragraphs");
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
    cfg.addProperty("graph.indexing.clearOnStartup", true);
    cfg.addProperty("graph.indexing.chunking.markdown.strategy", "heading");
    cfg.addProperty("graph.indexing.chunking.markdown.windowSizeTokens", 100);

    OneMcp mcp = new TestMcp(cfg, null);
    Handbook hb = newHandbookWithDocs(docs, mcp);
    ((TestMcp) mcp).setHandbook(hb);

    HandbookGraphService svc = new HandbookGraphService(mcp, driver);
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
