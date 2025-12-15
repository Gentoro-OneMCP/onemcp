package com.gentoro.onemcp.indexing.driver.arangodb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.DocumentCreateOptions;
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
  private static final String COLLECTION_ENTITIES = "entities";
  private static final String COLLECTION_OPERATIONS = "operations";
  private static final String COLLECTION_HAS_ENTITY = "hasEntity";
  private static final String COLLECTION_HAS_OPERATION = "hasOperation";
  private static final String COLLECTION_HAS_CHUNK = "hasChunk";
  private static final String GRAPH_NAME = "knowledgeGraph";

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
        oneMcp.configuration().getString("indexing.graph.arangodb.databasePrefix", DEFAULT_DB_PREFIX);
    this.databaseName = prefix + sanitize(this.handbookName);
  }

  @Override
  public void initialize() {
    if (initialized.get()) return;
    String host = oneMcp.configuration().getString("indexing.graph.arangodb.host", "localhost");
    int port = oneMcp.configuration().getInteger("indexing.graph.arangodb.port", 8529);
    String user = oneMcp.configuration().getString("indexing.graph.arangodb.user", "root");
    String password = oneMcp.configuration().getString("indexing.graph.arangodb.password", "");

    arango = new ArangoDB.Builder().host(host, port).user(user).password(password).build();
    if (!arango.getDatabases().contains(databaseName)) {
      arango.createDatabase(databaseName);
    }
    db = arango.db(databaseName);
    createCollectionIfNeeded(COLLECTION_NODES, CollectionType.DOCUMENT);
    createCollectionIfNeeded(COLLECTION_ENTITIES, CollectionType.DOCUMENT);
    createCollectionIfNeeded(COLLECTION_OPERATIONS, CollectionType.DOCUMENT);
    createCollectionIfNeeded(COLLECTION_HAS_ENTITY, CollectionType.EDGES);
    createCollectionIfNeeded(COLLECTION_HAS_OPERATION, CollectionType.EDGES);
    createCollectionIfNeeded(COLLECTION_HAS_CHUNK, CollectionType.EDGES);
    createGraphIfNeeded();
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

  private void createGraphIfNeeded() {
    try {
      if (!db.graph(GRAPH_NAME).exists()) {
        // Use REST API via HTTP request to create graph
        // This is the simplest approach that works with the Java driver
        String host = oneMcp.configuration().getString("indexing.graph.arangodb.host", "localhost");
        int port = oneMcp.configuration().getInteger("indexing.graph.arangodb.port", 8529);
        String user = oneMcp.configuration().getString("indexing.graph.arangodb.user", "root");
        String password = oneMcp.configuration().getString("indexing.graph.arangodb.password", "");
        
        String url = String.format("http://%s:%d/_db/%s/_api/gharial", host, port, databaseName);
        String jsonBody = String.format(
            "{\"name\":\"%s\",\"edgeDefinitions\":["
                + "{\"collection\":\"%s\",\"from\":[\"%s\"],\"to\":[\"%s\"]},"
                + "{\"collection\":\"%s\",\"from\":[\"%s\"],\"to\":[\"%s\"]},"
                + "{\"collection\":\"%s\",\"from\":[\"%s\"],\"to\":[\"%s\"]}"
                + "]}",
            GRAPH_NAME,
            COLLECTION_HAS_ENTITY, COLLECTION_NODES, COLLECTION_ENTITIES,
            COLLECTION_HAS_OPERATION, COLLECTION_NODES, COLLECTION_OPERATIONS,
            COLLECTION_HAS_CHUNK, COLLECTION_NODES, COLLECTION_NODES);
        
        try {
          java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
          java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
              .uri(java.net.URI.create(url))
              .header("Content-Type", "application/json")
              .header("Authorization", "Basic " + java.util.Base64.getEncoder()
                  .encodeToString((user + ":" + password).getBytes()))
              .POST(java.net.http.HttpRequest.BodyPublishers.ofString(jsonBody))
              .build();
          
          java.net.http.HttpResponse<String> response = client.send(request, 
              java.net.http.HttpResponse.BodyHandlers.ofString());
          
          if (response.statusCode() >= 200 && response.statusCode() < 300) {
            log.info("Created ArangoDB graph '{}' with edge collections: {}, {}, {}", 
                GRAPH_NAME, COLLECTION_HAS_ENTITY, COLLECTION_HAS_OPERATION, COLLECTION_HAS_CHUNK);
          } else {
            log.warn("Failed to create ArangoDB graph '{}': HTTP {} - {}", 
                GRAPH_NAME, response.statusCode(), response.body());
          }
        } catch (Exception e) {
          log.error("Failed to create ArangoDB graph '{}' via REST API: {}", 
              GRAPH_NAME, e.getMessage(), e);
        }
      } else {
        log.debug("ArangoDB graph '{}' already exists", GRAPH_NAME);
      }
    } catch (Exception e) {
      log.error("Failed to check/create ArangoDB graph '{}': {}", GRAPH_NAME, e.getMessage(), e);
    }
  }


  @Override
  public boolean isInitialized() {
    return initialized.get();
  }

  @Override
  public void clearAll() {
    db.collection(COLLECTION_NODES).truncate();
    db.collection(COLLECTION_ENTITIES).truncate();
    db.collection(COLLECTION_OPERATIONS).truncate();
    db.collection(COLLECTION_HAS_ENTITY).truncate();
    db.collection(COLLECTION_HAS_OPERATION).truncate();
    db.collection(COLLECTION_HAS_CHUNK).truncate();
  }

  @Override
  public void upsertNodes(List<GraphNodeRecord> nodes) {
    if (nodes == null || nodes.isEmpty()) return;
    List<Map<String, Object>> docs = new ArrayList<>();
    for (GraphNodeRecord n : nodes) {
      Map<String, Object> doc = new LinkedHashMap<>(n.toMap());
      doc.put("_key", sanitizeDocumentKey(n.getKey()));
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

    // Create edges to Entity and Operation vertices
    for (GraphNodeRecord n : nodes) {
      try {
        String nodeKey = sanitizeDocumentKey(n.getKey());
        String nodeId = COLLECTION_NODES + "/" + nodeKey;

        // Delete existing edges for this node
        String deleteEntityEdges =
            "FOR e IN "
                + COLLECTION_HAS_ENTITY
                + " FILTER e._from == @nodeId REMOVE e IN "
                + COLLECTION_HAS_ENTITY;
        String deleteOpEdges =
            "FOR e IN "
                + COLLECTION_HAS_OPERATION
                + " FILTER e._from == @nodeId REMOVE e IN "
                + COLLECTION_HAS_OPERATION;
        String deleteChunkEdges =
            "FOR e IN "
                + COLLECTION_HAS_CHUNK
                + " FILTER e._from == @nodeId OR e._to == @nodeId REMOVE e IN "
                + COLLECTION_HAS_CHUNK;
        db.query(deleteEntityEdges, Map.class, Map.of("nodeId", nodeId), new AqlQueryOptions());
        db.query(deleteOpEdges, Map.class, Map.of("nodeId", nodeId), new AqlQueryOptions());
        db.query(deleteChunkEdges, Map.class, Map.of("nodeId", nodeId), new AqlQueryOptions());

        // Create Entity vertices and edges
        @SuppressWarnings("unchecked")
        List<String> entities = n.getEntities();
        if (entities != null) {
          for (String entityName : entities) {
            if (entityName == null || entityName.isBlank()) continue;
            String entityKey = sanitizeDocumentKey(entityName);
            String entityId = COLLECTION_ENTITIES + "/" + entityKey;

            // Upsert entity vertex
            Map<String, Object> entityDoc = Map.of("_key", entityKey, "name", entityName);
            String upsertEntity =
                "UPSERT { _key: @key } INSERT @doc REPLACE @doc IN " + COLLECTION_ENTITIES;
            db.query(
                upsertEntity,
                Map.class,
                Map.of("key", entityKey, "doc", entityDoc),
                new AqlQueryOptions());

            // Create edge
            Map<String, Object> edge = Map.of("_from", nodeId, "_to", entityId);
            db.collection(COLLECTION_HAS_ENTITY)
                .insertDocument(edge, new DocumentCreateOptions());
          }
        }

        // Create Operation vertices and edges
        @SuppressWarnings("unchecked")
        List<String> operations = n.getOperations();
        if (operations != null) {
          for (String opName : operations) {
            if (opName == null || opName.isBlank()) continue;
            String opKey = sanitizeDocumentKey(opName);
            String opId = COLLECTION_OPERATIONS + "/" + opKey;

            // Upsert operation vertex
            Map<String, Object> opDoc = Map.of("_key", opKey, "name", opName);
            String upsertOp =
                "UPSERT { _key: @key } INSERT @doc REPLACE @doc IN " + COLLECTION_OPERATIONS;
            db.query(
                upsertOp, Map.class, Map.of("key", opKey, "doc", opDoc), new AqlQueryOptions());

            // Create edge
            Map<String, Object> edge = Map.of("_from", nodeId, "_to", opId);
            db.collection(COLLECTION_HAS_OPERATION)
                .insertDocument(edge, new DocumentCreateOptions());
          }
        }

        // Create HAS_CHUNK edges: if this node has a parentDocumentKey, create edge from document to chunk
        String parentDocKey = n.getParentDocumentKey();
        if (parentDocKey != null && !parentDocKey.isBlank()) {
          try {
            String parentDocKeySanitized = sanitizeDocumentKey(parentDocKey);
            String parentDocId = COLLECTION_NODES + "/" + parentDocKeySanitized;
            // Verify parent document exists
            if (db.collection(COLLECTION_NODES).documentExists(parentDocKeySanitized)) {
              Map<String, Object> chunkEdge = Map.of("_from", parentDocId, "_to", nodeId);
              db.collection(COLLECTION_HAS_CHUNK)
                  .insertDocument(chunkEdge, new DocumentCreateOptions());
            } else {
              log.warn(
                  "Parent document '{}' not found for chunk '{}', skipping HAS_CHUNK edge",
                  parentDocKey,
                  n.getKey());
            }
          } catch (Exception e) {
            log.warn(
                "Failed to create HAS_CHUNK edge for chunk '{}' to parent '{}': {}",
                n.getKey(),
                parentDocKey,
                e.getMessage());
          }
        }
      } catch (Exception e) {
        log.warn(
            "Failed to create edges for node '{}': {}", n.getKey(), e.getMessage(), e);
      }
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

    // Use graph traversal to find nodes connected to requested entities
    List<Map> raw = Collections.emptyList();
    ArangoCursor<Map> cursor = null;
    try {
      // Build entity keys for graph traversal
      List<String> entityKeys =
          entities.stream().map(ArangoGraphDriver::sanitizeDocumentKey).collect(Collectors.toList());
      Map<String, Object> bind = Map.of("entityKeys", entityKeys);

      // Graph traversal: find nodes connected to entities via HasEntity edges
      String aql =
          "FOR entity IN "
              + COLLECTION_ENTITIES
              + " FILTER entity._key IN @entityKeys "
              + "FOR v, e, p IN 1..1 INBOUND entity "
              + COLLECTION_HAS_ENTITY
              + " "
              + "FOR node IN "
              + COLLECTION_NODES
              + " FILTER node._id == v._id "
              + "RETURN DISTINCT node";
      cursor = db.query(aql, Map.class, bind, new AqlQueryOptions());
      raw = cursor.asListRemaining();
    } catch (Exception e) {
      log.error("Arango queryByContext graph traversal failed: {}", e.getMessage(), e);
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

    // If no direct matches, fallback: find document nodes that match entities, then include their chunks
    if (result.isEmpty()) {
      try {
        // Build entity keys for document lookup
        List<String> entityKeys =
            entities.stream().map(ArangoGraphDriver::sanitizeDocumentKey).collect(Collectors.toList());
        Map<String, Object> fallbackBind = Map.of("entityKeys", entityKeys);
        
        // Find document nodes matching entities
        String findDocsAql =
            "FOR doc IN "
                + COLLECTION_NODES
                + " FILTER doc.nodeType == 'DOCUMENT' "
                + "LET docEntities = doc.entities "
                + "FILTER LENGTH(INTERSECTION(docEntities, @entityKeys)) > 0 "
                + "RETURN doc._key";
        ArangoCursor<String> docCursor =
            db.query(findDocsAql, String.class, fallbackBind, new AqlQueryOptions());
        List<String> matchingDocKeys = docCursor.asListRemaining();

        if (!matchingDocKeys.isEmpty()) {
          // Find chunks linked to these documents via HAS_CHUNK edges
          String findChunksAql =
              "FOR docKey IN @docKeys "
                  + "LET docId = CONCAT('"
                  + COLLECTION_NODES
                  + "/', docKey) "
                  + "FOR v, e, p IN 1..1 OUTBOUND docId "
                  + COLLECTION_HAS_CHUNK
                  + " "
                  + "FOR chunk IN "
                  + COLLECTION_NODES
                  + " FILTER chunk._id == v._id "
                  + "RETURN DISTINCT chunk";
          Map<String, Object> chunkBind = Map.of("docKeys", matchingDocKeys);
          ArangoCursor<Map> chunkCursor =
              db.query(findChunksAql, Map.class, chunkBind, new AqlQueryOptions());
          List<Map> chunkResults = chunkCursor.asListRemaining();
          for (Object o : chunkResults) {
            @SuppressWarnings("unchecked")
            Map<String, Object> chunkMap = (Map<String, Object>) o;
            result.add(new LinkedHashMap<>(chunkMap));
          }
        }
      } catch (Exception e) {
        log.warn("Arango queryByContext fallback failed: {}", e.getMessage());
      }
    }

    return result;
  }

  @Override
  public void deleteNodesByKeys(List<String> keys) {
    if (keys == null || keys.isEmpty()) return;
    for (String k : keys) {
      String sanitizedKey = sanitizeDocumentKey(k);
      db.collection(COLLECTION_NODES).deleteDocument(sanitizedKey, new DocumentDeleteOptions());
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

  /**
   * Sanitizes a document key to be ArangoDB-compliant.
   *
   * <p>ArangoDB document keys (_key) must:
   * <ul>
   *   <li>Only contain letters, numbers, dashes, and underscores</li>
   *   <li>Not start with a number</li>
   *   <li>Be 1-254 characters long</li>
   * </ul>
   *
   * @param key the original key (may contain pipes, slashes, etc.)
   * @return sanitized key valid for ArangoDB _key field
   */
  private static String sanitizeDocumentKey(String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Document key cannot be null or empty");
    }
    // Replace pipe separators and other invalid characters with underscores
    String sanitized = key.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    // Ensure it doesn't start with a number
    if (sanitized.matches("^[0-9].*")) {
      sanitized = "k_" + sanitized;
    }
    // Ensure it doesn't start with underscore (though ArangoDB allows it, we avoid it for clarity)
    if (sanitized.startsWith("_")) {
      sanitized = "k" + sanitized;
    }
    // Truncate to 254 characters (ArangoDB limit)
    if (sanitized.length() > 254) {
      sanitized = sanitized.substring(0, 254);
    }
    return sanitized;
  }
}
