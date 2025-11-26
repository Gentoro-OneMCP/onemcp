package com.gentoro.onemcp.indexing.driver.arangodb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * ArangoDB implementation of the v2 GraphDriver.
 *
 * <p>Stores all nodes in a single document collection "nodes" within a handbook-specific database.
 */
public class ArangoGraphDriver implements GraphDriver {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ArangoGraphDriver.class);

  private static final String DEFAULT_DB_PREFIX = "onemcp_";
  private static final String COLLECTION_NODES = "nodes";

  private final OneMcp oneMcp;
  private final String handbookName;
  private final String databaseName;
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private ArangoDB arango;
  private ArangoDatabase db;

  public ArangoGraphDriver(OneMcp oneMcp, String handbookName) {
    this.oneMcp = Objects.requireNonNull(oneMcp, "oneMcp");
    this.handbookName = handbookName != null ? handbookName : "default";
    String prefix =
        oneMcp.configuration().getString("graph.arangodb.databasePrefix", DEFAULT_DB_PREFIX);
    this.databaseName = prefix + sanitize(this.handbookName);
  }

  @Override
  public void initialize() {
    if (initialized.get()) return;
    String host = oneMcp.configuration().getString("graph.arangodb.host", "localhost");
    int port = oneMcp.configuration().getInteger("graph.arangodb.port", 8529);
    String user = oneMcp.configuration().getString("graph.arangodb.user", "root");
    String password = oneMcp.configuration().getString("graph.arangodb.password", "");

    arango = new ArangoDB.Builder().host(host, port).user(user).password(password).build();
    if (!arango.getDatabases().contains(databaseName)) {
      arango.createDatabase(databaseName);
    }
    db = arango.db(databaseName);
    createCollectionIfNeeded(COLLECTION_NODES, CollectionType.DOCUMENT);
    initialized.set(true);
    log.info(
        "ArangoGraphDriver initialized database '{}' for handbook '{}'",
        databaseName,
        handbookName);
  }

  private void createCollectionIfNeeded(String name, CollectionType type) {
    if (!db.collection(name).exists()) {
      db.createCollection(name, new CollectionCreateOptions().type(type));
    }
  }

  @Override
  public boolean isInitialized() {
    return initialized.get();
  }

  @Override
  public void clearAll() {
    db.collection(COLLECTION_NODES).truncate();
  }

  @Override
  public void upsertNodes(List<GraphNodeRecord> nodes) {
    if (nodes == null || nodes.isEmpty()) return;
    List<Map<String, Object>> docs = new ArrayList<>();
    for (GraphNodeRecord n : nodes) {
      Map<String, Object> doc = new LinkedHashMap<>(n.toMap());
      doc.put("_key", n.getKey());
      docs.add(doc);
    }
    String aql =
        "FOR doc IN @docs UPSERT { _key: doc._key } INSERT doc REPLACE doc IN " + COLLECTION_NODES;
    Map<String, Object> bind = Map.of("docs", docs);
    // arangodb-java-driver v7 uses signature: query(String, Class<T>, Map<String,?>,
    // AqlQueryOptions)
    try (ArangoCursor<Map> cursor = db.query(aql, Map.class, bind, new AqlQueryOptions())) {
      // consume to ensure execution
      while (cursor.hasNext()) cursor.next();
    } catch (Exception e) {
      log.warn("Arango upsertNodes encountered an issue: {}", e.getMessage());
    }
  }

  @Override
  public List<Map<String, Object>> queryByContext(List<GraphContextTuple> contextTuples) {
    if (contextTuples == null || contextTuples.isEmpty()) {
      // return all
      String aql = "FOR n IN nodes RETURN n";
      ArangoCursor<Map> cursor = null;
      try {
        cursor = db.query(aql, Map.class, Collections.emptyMap(), new AqlQueryOptions());
        return cursor.asListRemaining().stream()
            .map(m -> (Map<String, Object>) m)
            .map(LinkedHashMap::new)
            .collect(Collectors.toList());
      } catch (Exception e) {
        log.warn("Arango queryByContext(all) failed: {}", e.getMessage());
        return Collections.emptyList();
      } finally {
        if (cursor != null)
          try {
            cursor.close();
          } catch (Exception ignore) {
          }
      }
    }

    // Build lookup maps like InMemory implementation
    Set<String> entities = new HashSet<>();
    Map<String, Set<String>> opsByEntity = new HashMap<>();
    for (GraphContextTuple t : contextTuples) {
      entities.add(t.getEntity());
      opsByEntity.computeIfAbsent(t.getEntity(), k -> new HashSet<>()).addAll(t.getOperations());
    }

    Map<String, Object> bind = Map.of("entities", entities);
    String aql = "FOR n IN nodes FILTER LENGTH(INTERSECTION(n.entities, @entities)) > 0 RETURN n";
    List<Map> raw = Collections.emptyList();
    ArangoCursor<Map> cursor = null;
    try {
      cursor = db.query(aql, Map.class, bind, new AqlQueryOptions());
      raw = cursor.asListRemaining();
    } catch (Exception e) {
      log.warn("Arango queryByContext(filter) failed: {}", e.getMessage());
      raw = Collections.emptyList();
    } finally {
      if (cursor != null)
        try {
          cursor.close();
        } catch (Exception ignore) {
        }
    }

    // Post-filter with the same logic as InMemory
    List<Map<String, Object>> result = new ArrayList<>();
    for (Object o : raw) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) o;
      @SuppressWarnings("unchecked")
      List<String> nodeEntities =
          (List<String>) map.getOrDefault("entities", Collections.emptyList());
      @SuppressWarnings("unchecked")
      List<String> nodeOps = (List<String>) map.getOrDefault("operations", Collections.emptyList());

      Optional<String> entityMatch = nodeEntities.stream().filter(entities::contains).findFirst();
      if (entityMatch.isEmpty()) continue;
      String matchedEntity = entityMatch.get();
      if (nodeOps == null || nodeOps.isEmpty()) {
        result.add(new LinkedHashMap<>(map));
        continue;
      }
      Set<String> requested = opsByEntity.getOrDefault(matchedEntity, Collections.emptySet());
      if (requested.isEmpty()) {
        result.add(new LinkedHashMap<>(map));
        continue;
      }
      boolean ok = false;
      for (String op : nodeOps) {
        if (requested.contains(op)) {
          ok = true;
          break;
        }
      }
      if (ok) result.add(new LinkedHashMap<>(map));
    }
    return result;
  }

  @Override
  public void deleteNodesByKeys(List<String> keys) {
    if (keys == null || keys.isEmpty()) return;
    for (String k : keys) {
      db.collection(COLLECTION_NODES).deleteDocument(k, new DocumentDeleteOptions());
    }
  }

  @Override
  public String getDriverName() {
    return "arangodb";
  }

  @Override
  public String getHandbookName() {
    return handbookName;
  }

  @Override
  public void shutdown() {
    initialized.set(false);
    try {
      if (arango != null) arango.shutdown();
    } catch (Exception ignored) {
    }
  }

  private static String sanitize(String name) {
    String s = name.toLowerCase().replaceAll("[^a-z0-9_\\-]", "_");
    if (!s.matches("^[a-z].*")) s = "h_" + s;
    if (s.length() > 64) s = s.substring(0, 64);
    return s;
  }
}
