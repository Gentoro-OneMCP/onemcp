package com.gentoro.onemcp.indexing.driver.orient;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import com.gentoro.onemcp.utility.FileUtility;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;

public class OrientGraphIntegrationTest {
  private OrientGraphDriver driver;

  static class FakeOneMcp extends OneMcp {
    public FakeOneMcp() {
      super(new String[0]);
    }

    @Override
    public org.apache.commons.configuration2.Configuration configuration() {
      var cfg = new org.apache.commons.configuration2.MapConfiguration(new HashMap<>());
      cfg.addProperty("indexing.graph.orient.rootDir", "target/integration-db");
      cfg.addProperty("indexing.graph.clearOnStartup", true);
      return cfg;
    }
  }

  @BeforeEach
  void init() {
    driver = new OrientGraphDriver(new FakeOneMcp(), "itest");
    // With the upgraded OrientDB driver, initialization should succeed on supported JDKs.
    // If the environment is incompatible, let the test surface the error instead of skipping.
    driver.initialize();
  }

  @AfterEach
  void after() {
    if (driver != null && driver.isInitialized()) driver.shutdown();
    File f = new File("target/integration-db");
    if (f.exists()) {
      try {
        FileUtility.deleteDir(Path.of(f.getPath()), true);
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  void complexEntityOperationQuery() {
    driver.upsertNodes(
        List.of(
            new GraphNodeRecord("n1", KnowledgeNodeType.DOCS_CHUNK)
                .setEntities(List.of("User"))
                .setOperations(List.of("GET", "POST")),
            new GraphNodeRecord("n2", KnowledgeNodeType.DOCS_CHUNK)
                .setEntities(List.of("User"))
                .setOperations(List.of("PATCH")),
            new GraphNodeRecord("n3", KnowledgeNodeType.DOCS_CHUNK)
                .setEntities(List.of("User"))
                .setOperations(List.of()),
            new GraphNodeRecord("n4", KnowledgeNodeType.DOCS_CHUNK)
                .setEntities(List.of("Order"))
                .setOperations(List.of("GET"))));

    var ctx =
        List.of(
            new GraphContextTuple("User", List.of("GET")),
            new GraphContextTuple("Order", List.of("GET")));

    var res = driver.queryByContext(ctx);

    // n1 matches ("User", GET)
    // n3 matches because no ops
    // n4 matches ("Order", GET)
    assertEquals(3, res.size());
  }
}
