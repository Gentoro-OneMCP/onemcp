package com.gentoro.onemcp.indexing.driver.neo4j;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * Neo4j implementation of the v2 GraphDriver using the embedded database. Stores all nodes as
 * (:KnowledgeNode {key, ...props}).
 */
public class Neo4jGraphDriver implements GraphDriver {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(Neo4jGraphDriver.class);

  private final OneMcp oneMcp;
  private final String handbookName;

  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private DatabaseManagementService managementService;
  private GraphDatabaseService graphDb;
  private String database; // resolved database name
  private File rootDir;

  public Neo4jGraphDriver(OneMcp oneMcp, String handbookName) {
    this.oneMcp = Objects.requireNonNull(oneMcp, "oneMcp");
    this.handbookName = handbookName != null ? handbookName : "default";
  }

  @Override
  public void initialize() {
    if (initialized.get()) return;
    log.trace("Initializing Neo4jGraphDriver for handbook '{}'", handbookName);
    try {
      // Embedded DB configuration
      String root =
          oneMcp
              .configuration()
              .getString("graph.neo4j.rootDir", new File("data/neo4j").getAbsolutePath());
      String dbPrefix = oneMcp.configuration().getString("graph.neo4j.databasePrefix", "onemcp-");
      String configuredDb = oneMcp.configuration().getString("graph.neo4j.database", "");
      this.database =
          (configuredDb != null && !configuredDb.isBlank())
              ? configuredDb
              : (dbPrefix + handbookName);
      this.rootDir = Path.of(root, this.database).toFile();
      if (!rootDir.exists() && !rootDir.mkdirs()) {
        throw new IoException("Unable to create Neo4j root directory: " + rootDir);
      }

      // Start embedded Neo4j DBMS
      log.info("Starting Neo4j database at {}", rootDir);
      managementService =
          new DatabaseManagementServiceBuilder(rootDir.toPath())
              .setConfig(GraphDatabaseSettings.initial_default_database, database)
              .build();
      try {
        // Open the database
        log.info("Opening Neo4j database '{}'", database);
        graphDb = managementService.database(database);
      } catch (Exception ex) {
        throw new HandbookException(
            "Failed to open/create Neo4j database '" + database + "' at " + rootDir, ex);
      }

      // Ensure constraint on key
      try (Transaction tx = graphDb.beginTx()) {
        tx.execute(
            "CREATE CONSTRAINT knowledge_key IF NOT EXISTS FOR (n:KnowledgeNode) REQUIRE n.key IS UNIQUE");
        tx.commit();
      }
      initialized.set(true);
    } finally {
      log.trace("Neo4jGraphDriver initialized for handbook '{}'", handbookName);
    }
  }

  private Transaction openTx(boolean write) {
    return graphDb.beginTx();
  }

  @Override
  public boolean isInitialized() {
    return initialized.get();
  }

  @Override
  public void clearAll() {
    try (Transaction tx = openTx(true)) {
      tx.execute("MATCH (n:KnowledgeNode) DETACH DELETE n");
      tx.commit();
    }
  }

  @Override
  public void upsertNodes(List<GraphNodeRecord> nodes) {
    if (nodes == null || nodes.isEmpty()) return;
    List<Map<String, Object>> rows = new ArrayList<>();
    for (GraphNodeRecord n : nodes) {
      Map<String, Object> m = new LinkedHashMap<>(n.toMap());
      // Ensure types for list properties
      m.put("entities", new ArrayList<>((Collection<?>) m.getOrDefault("entities", List.of())));
      m.put("operations", new ArrayList<>((Collection<?>) m.getOrDefault("operations", List.of())));
      rows.add(m);
    }
    String cypher =
        "UNWIND $rows AS row "
            + "MERGE (n:KnowledgeNode {key: row.key}) "
            + "SET n.nodeType = row.nodeType, n.apiSlug = row.apiSlug, n.operationId = row.operationId, "
            + "    n.content = row.content, n.contentFormat = row.contentFormat, n.docPath = row.docPath, "
            + "    n.title = row.title, n.summary = row.summary, n.entities = row.entities, n.operations = row.operations";
    try (Transaction tx = openTx(true)) {
      tx.execute(cypher, Map.of("rows", rows));
      tx.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryByContext(List<GraphContextTuple> contextTuples) {
    try (Transaction tx = openTx(false)) {
      if (contextTuples == null || contextTuples.isEmpty()) {
        String all = "MATCH (n:KnowledgeNode) RETURN properties(n) AS n";
        Result result = tx.execute(all);
        List<Map<String, Object>> out = new ArrayList<>();
        while (result.hasNext()) {
          Map<String, Object> row = result.next();
          @SuppressWarnings("unchecked")
          Map<String, Object> m = (Map<String, Object>) row.get("n");
          out.add(toLinkedMap(m));
        }
        return out;
      }

      // Build entity set; ops handled post-filter similar to in-memory
      Set<String> entities = new HashSet<>();
      Map<String, Set<String>> opsByEntity = new HashMap<>();
      for (GraphContextTuple t : contextTuples) {
        entities.add(t.getEntity());
        opsByEntity.computeIfAbsent(t.getEntity(), k -> new HashSet<>()).addAll(t.getOperations());
      }

      String cypher =
          "MATCH (n:KnowledgeNode) \n"
              + "WITH n, n.entities AS ents \n"
              + "WHERE size([x IN ents WHERE x IN $entities]) > 0 \n"
              + "RETURN properties(n) AS n";
      Result result = tx.execute(cypher, Map.of("entities", new ArrayList<>(entities)));

      // Post-filter by operations semantics
      List<Map<String, Object>> filtered = new ArrayList<>();
      while (result.hasNext()) {
        Map<String, Object> row = result.next();
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) row.get("n");

        List<String> nodeEntities = new ArrayList<>();
        if (map.containsKey("entities")) {
          if (map.get("entities") instanceof List<?>) {
            nodeEntities.addAll((List<String>) map.get("entities"));
          } else if (map.get("entities").getClass().isArray()) {
            nodeEntities.addAll(Arrays.asList((String[]) map.get("entities")));
          }
        }
        @SuppressWarnings("unchecked")
        List<String> nodeOps = new ArrayList<>();
        if (map.containsKey("operations")) {
          if (map.get("operations") instanceof List<?>) {
            nodeEntities.addAll((List<String>) map.get("operations"));
          } else if (map.get("operations").getClass().isArray()) {
            nodeEntities.addAll(Arrays.asList((String[]) map.get("operations")));
          }
        }
        Optional<String> entityMatch = nodeEntities.stream().filter(entities::contains).findFirst();
        if (entityMatch.isEmpty()) continue;
        String matchedEntity = entityMatch.get();
        if (nodeOps == null || nodeOps.isEmpty()) {
          filtered.add(toLinkedMap(map));
          continue;
        }
        Set<String> requested = opsByEntity.getOrDefault(matchedEntity, Collections.emptySet());
        if (requested.isEmpty()) {
          filtered.add(toLinkedMap(map));
          continue;
        }
        boolean ok = false;
        for (String op : nodeOps) {
          if (requested.contains(op)) {
            ok = true;
            break;
          }
        }
        if (ok) filtered.add(toLinkedMap(map));
      }
      return filtered;
    }
  }

  private static Map<String, Object> toLinkedMap(Map<String, Object> m) {
    return new LinkedHashMap<>(m);
  }

  @Override
  public void deleteNodesByKeys(List<String> keys) {
    if (keys == null || keys.isEmpty()) return;
    String cypher = "UNWIND $keys AS k MATCH (n:KnowledgeNode {key: k}) DETACH DELETE n";
    try (Transaction tx = openTx(true)) {
      tx.execute(cypher, Map.of("keys", keys));
      tx.commit();
    }
  }

  @Override
  public String getDriverName() {
    return "neo4j";
  }

  @Override
  public String getHandbookName() {
    return handbookName;
  }

  @Override
  public void shutdown() {
    initialized.set(false);
    try {
      if (managementService != null) {
        managementService.shutdown();
      }
    } catch (Exception ignore) {
    }
  }
}
