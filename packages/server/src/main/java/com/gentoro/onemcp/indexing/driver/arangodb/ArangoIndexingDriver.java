package com.gentoro.onemcp.indexing.driver.arangodb;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.CollectionCreateOptions;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ConfigException;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.indexing.driver.GraphIndexingDriver;
import com.gentoro.onemcp.indexing.graph.GraphEdge;
import com.gentoro.onemcp.indexing.graph.nodes.GraphNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ArangoDB service for indexing knowledge base data in graph format.
 *
 * <p>This service provides features for indexing handbook data as a graph with vertices (nodes) and
 * edges (relationships) in ArangoDB. It supports multiple node types (entities, operations,
 * examples) and various relationship types.
 *
 * <p>Each handbook gets its own dedicated ArangoDB database (named: onemcp_{handbook_name}),
 * ensuring complete isolation between different handbooks. This allows multiple handbooks to be
 * indexed on the same ArangoDB instance without data conflicts.
 *
 * <p>The graph structure enables semantic querying and retrieval based on relationships between
 * different knowledge base elements.
 */
public class ArangoIndexingDriver implements GraphIndexingDriver {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ArangoIndexingDriver.class);

  private static final String DATABASE_PREFIX = "onemcp_";
  private static final String CONFIG_PREFIX = "graph.indexing.arangodb.";

  // Collection names for different node types
  private static final String ENTITIES_COLLECTION = "entities";
  private static final String OPERATIONS_COLLECTION = "operations";
  private static final String EXAMPLES_COLLECTION = "examples";
  private static final String FIELDS_COLLECTION = "fields";
  private static final String DOCUMENTATIONS_COLLECTION = "documentations";

  // Edge collections for different relationship types
  private static final String EDGES_COLLECTION = "edges";

  private final OneMcp oneMcp;
  private final String handbookName;
  private final String databaseName;
  private ArangoDB arangoDB;
  private ArangoDatabase database;
  private boolean initialized = false;

  /**
   * Create a new ArangoDB indexing driver bound to the provided OneMcp context.
   *
   * @param oneMcp Main entry point for OneMCP application context
   * @param handbookName Name of the handbook being indexed
   */
  public ArangoIndexingDriver(OneMcp oneMcp, String handbookName) {
    this.oneMcp = oneMcp;
    this.handbookName = handbookName;
    this.databaseName = deriveDatabaseName(handbookName);
  }

  /**
   * Derive a database name from the handbook name. Creates a unique database name per handbook:
   * onemcp_{sanitized_handbook_name}
   *
   * @param handbookName the handbook name
   * @return sanitized database name
   */
  private String deriveDatabaseName(String handbookName) {
    if (handbookName == null || handbookName.trim().isEmpty()) {
      log.warn("Handbook name is null or empty, using default database name");
      return DATABASE_PREFIX + "default";
    }

    String sanitized = sanitizeDatabaseName(handbookName);
    return DATABASE_PREFIX + sanitized;
  }

  /**
   * Sanitize a database name to be valid for ArangoDB. ArangoDB database names: a-z, A-Z, 0-9, _,
   * -, must start with letter
   *
   * @param name the name to sanitize
   * @return sanitized database name
   */
  private String sanitizeDatabaseName(String name) {
    // ArangoDB database names: alphanumeric, underscore, hyphen
    // Must start with letter
    String sanitized = name.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();

    // Ensure it starts with a letter
    if (!sanitized.matches("^[a-zA-Z].*")) {
      sanitized = "handbook_" + sanitized;
    }

    // Limit length (ArangoDB has reasonable limits, but be safe)
    if (sanitized.length() > 64) {
      sanitized = sanitized.substring(0, 64);
    }

    return sanitized;
  }

  /**
   * Initialize the ArangoDB connection and create necessary collections.
   *
   * @throws ConfigException if configuration is missing or invalid
   * @throws IoException if connection or initialization fails
   */
  @Override
  public void initialize() {
    if (initialized) {
      log.debug("ArangoDB service already initialized");
      return;
    }

    String host = oneMcp.configuration().getString(CONFIG_PREFIX + "host", "localhost");
    Integer port = oneMcp.configuration().getInteger(CONFIG_PREFIX + "port", 8529);
    String user = oneMcp.configuration().getString(CONFIG_PREFIX + "user", "root");
    String password = oneMcp.configuration().getString(CONFIG_PREFIX + "password", "");
    Boolean enabled = oneMcp.configuration().getBoolean(CONFIG_PREFIX + "enabled", false);

    if (!enabled) {
      log.info("ArangoDB indexing is disabled in configuration");
      return;
    }

    log.info(
        "Initializing ArangoDB connection: {}:{} for handbook: {} (database: {})",
        host,
        port,
        handbookName,
        databaseName);

    try {
      arangoDB = new ArangoDB.Builder().host(host, port).user(user).password(password).build();

      // Create handbook-specific database if it doesn't exist
      if (!arangoDB.getDatabases().contains(databaseName)) {
        log.info("Creating ArangoDB database for handbook '{}': {}", handbookName, databaseName);
        arangoDB.createDatabase(databaseName);
      } else {
        log.debug(
            "ArangoDB database '{}' already exists for handbook '{}'", databaseName, handbookName);
      }

      database = arangoDB.db(databaseName);

      // Create document collections for different node types
      createCollectionIfNotExists(ENTITIES_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(OPERATIONS_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(EXAMPLES_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(FIELDS_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(DOCUMENTATIONS_COLLECTION, CollectionType.DOCUMENT);

      // Create edge collection for relationships
      createCollectionIfNotExists(EDGES_COLLECTION, CollectionType.EDGES);

      // Note: Named graph will be created after data is indexed (or cleared)
      // This ensures the graph is created with the correct handbook name
      // Indexes are created automatically by ArangoDB on _key field

      initialized = true;
      log.info("ArangoDB service initialized successfully");
    } catch (Exception e) {
      throw new IoException("Failed to initialize ArangoDB service", e);
    }
  }

  /**
   * Create a collection if it doesn't exist.
   *
   * @param name collection name
   * @param type collection type (DOCUMENT or EDGES)
   */
  private void createCollectionIfNotExists(String name, CollectionType type) {
    try {
      if (!database.collection(name).exists()) {
        log.info("Creating collection: {} (type: {})", name, type);
        CollectionCreateOptions options = new CollectionCreateOptions().type(type);
        database.createCollection(name, options);
      }
    } catch (Exception e) {
      throw new IoException("Failed to create collection: " + name, e);
    }
  }

  /**
   * Create a named graph for visualization in ArangoDB UI.
   *
   * <p>Creates a General Graph that includes all vertex collections (entities, operations,
   * examples, fields) and the edges collection. This allows visualization in the Graphs section of
   * ArangoDB UI with proper edge label support.
   */
  private void createNamedGraph() {
    try {
      // Use handbook name from constructor
      String actualHandbookName =
          (handbookName != null && !handbookName.trim().isEmpty())
              ? handbookName
              : "onemcp_handbook";

      String graphName = sanitizeGraphName(actualHandbookName);

      // Check if graph already exists
      if (database.graph(graphName).exists()) {
        log.info("Graph '{}' already exists, skipping creation", graphName);
        return;
      }

      // Define all vertex collections as arrays
      String[] vertexCollections =
          new String[] {
            ENTITIES_COLLECTION,
            OPERATIONS_COLLECTION,
            EXAMPLES_COLLECTION,
            FIELDS_COLLECTION,
            DOCUMENTATIONS_COLLECTION
          };

      // Create edge definition - edges can connect any vertex collection to any other
      EdgeDefinition edgeDefinition = new EdgeDefinition();
      edgeDefinition.collection(EDGES_COLLECTION);
      edgeDefinition.from(vertexCollections);
      edgeDefinition.to(vertexCollections);

      List<EdgeDefinition> edgeDefinitions = new ArrayList<>();
      edgeDefinitions.add(edgeDefinition);

      // Create the graph with edge definitions
      database.createGraph(graphName, edgeDefinitions);
      log.info(
          "Created named graph '{}' with {} vertex collections and edge collection '{}'",
          graphName,
          vertexCollections.length,
          EDGES_COLLECTION);
    } catch (Exception e) {
      log.warn("Failed to create named graph, continuing without formal graph definition", e);
      log.info("You can still visualize using queries in the Queries tab");
    }
  }

  /**
   * Store a graph node in the appropriate collection based on its type.
   *
   * @param node the graph node to store
   * @throws IoException if indexing operation fails
   */
  @Override
  public void storeNode(GraphNode node) {
    if (!initialized || database == null) {
      log.warn("ArangoDB service not initialized, skipping node storage: {}", node.getKey());
      return;
    }

    try {
      String collection = getCollectionForNodeType(node.getNodeType());
      log.trace("Storing node: {} in collection: {}", node.getKey(), collection);

      Map<String, Object> data = node.toMap();
      // Sanitize the _key field for ArangoDB (replace | with _ and <> with _to_)
      String originalKey = (String) data.get("_key");
      String sanitizedKey = null;
      if (originalKey != null) {
        sanitizedKey = sanitizeKeyForArangoDB(originalKey);
        data.put("_key", sanitizedKey);
        log.trace("Sanitized key: {} -> {}", originalKey, sanitizedKey);
      }

      // Use upsert to handle duplicate nodes gracefully (insert or replace if exists)
      try {
        database.collection(collection).insertDocument(data);
        log.debug("Successfully stored node: {} (type: {})", node.getKey(), node.getNodeType());
        // Log category if present for debugging
        if (data.containsKey("category") && data.get("category") != null) {
          log.trace("Stored node with category: {} = {}", node.getKey(), data.get("category"));
        }
      } catch (com.arangodb.ArangoDBException e) {
        // If node already exists (unique constraint violated), replace it to ensure all fields
        // (including category) are updated
        if (e.getErrorNum() == 1210 && sanitizedKey != null) { // 1210 = unique constraint violated
          log.debug(
              "Node already exists, replacing: {} (type: {})", node.getKey(), node.getNodeType());
          database.collection(collection).replaceDocument(sanitizedKey, data);
          // Log category if present for debugging
          if (data.containsKey("category") && data.get("category") != null) {
            log.trace("Replaced node with category: {} = {}", node.getKey(), data.get("category"));
          }
        } else {
          throw e;
        }
      }
    } catch (Exception e) {
      throw new IoException("Failed to store node: " + node.getKey(), e);
    }
  }

  /**
   * Store multiple graph nodes in a batch operation.
   *
   * @param nodes list of nodes to store
   * @throws IoException if batch operation fails
   */
  @Override
  public void storeNodes(List<GraphNode> nodes) {
    if (!initialized || database == null) {
      log.warn("ArangoDB service not initialized, skipping batch node storage");
      return;
    }

    log.debug("Storing {} nodes in batch", nodes.size());
    for (GraphNode node : nodes) {
      storeNode(node);
    }
  }

  /**
   * Store an edge (relationship) between two nodes in the graph.
   *
   * @param edge the edge to store
   * @throws IoException if indexing operation fails
   */
  @Override
  public void storeEdge(GraphEdge edge) {
    if (!initialized || database == null) {
      log.warn("ArangoDB service not initialized, skipping edge storage");
      return;
    }

    try {
      // Early validation: reject edges with invalid key formats
      String fromKey = edge.getFromKey();
      String toKey = edge.getToKey();

      if (fromKey == null || toKey == null || fromKey.isEmpty() || toKey.isEmpty()) {
        log.warn("Skipping edge with null/empty keys: {} -> {}", fromKey, toKey);
        return;
      }

      // Reject generic type names that aren't valid node keys
      if (!isValidNodeKey(toKey) || !isValidNodeKey(fromKey)) {
        log.warn(
            "Skipping edge with invalid key format (generic type?): {} -> {} (type: {})",
            fromKey,
            toKey,
            edge.getEdgeType());
        return;
      }

      // Sanitize keys for ArangoDB (replace | with _ and <> with _to_)
      String sanitizedFromKey = sanitizeKeyForArangoDB(fromKey);
      String sanitizedToKey = sanitizeKeyForArangoDB(toKey);
      String edgeKey = sanitizeKeyForArangoDB(sanitizedFromKey + "<>" + sanitizedToKey);

      log.trace("Storing edge: {} -> {} (type: {})", fromKey, toKey, edge.getEdgeType());
      log.trace(
          "Sanitized edge keys: {} -> {} (from: {}, to: {})",
          fromKey + "<>" + toKey,
          edgeKey,
          sanitizedFromKey,
          sanitizedToKey);

      Map<String, Object> edgeData = new HashMap<>(edge.toMap());
      edgeData.put("_key", edgeKey);

      // Determine source and target collections (using sanitized keys)
      String fromCollection = determineCollectionFromKey(sanitizedFromKey);
      String toCollection = determineCollectionFromKey(sanitizedToKey);

      String fromRef = fromCollection + "/" + sanitizedFromKey;
      String toRef = toCollection + "/" + sanitizedToKey;

      edgeData.put("_from", fromRef);
      edgeData.put("_to", toRef);

      // Validate that referenced documents exist before creating edge
      if (!documentExists(fromCollection, sanitizedFromKey)) {
        log.warn(
            "Source document does not exist for edge: {} (collection: {}, key: {})",
            edge.getFromKey(),
            fromCollection,
            sanitizedFromKey);
        // Don't throw - just skip this edge to prevent graph corruption
        return;
      }

      if (!documentExists(toCollection, sanitizedToKey)) {
        log.warn(
            "Target document does not exist for edge: {} (collection: {}, key: {})",
            edge.getToKey(),
            toCollection,
            sanitizedToKey);
        // Don't throw - just skip this edge to prevent graph corruption
        return;
      }

      // Use upsert to handle duplicate edges gracefully (insert or update if exists)
      try {
        database.collection(EDGES_COLLECTION).insertDocument(edgeData);
        log.debug("Successfully stored edge: {} -> {}", edge.getFromKey(), edge.getToKey());
      } catch (com.arangodb.ArangoDBException e) {
        // If edge already exists (unique constraint violation), update it instead
        if (e.getErrorNum() == 1210) { // 1210 = unique constraint violated
          log.debug("Edge already exists, updating: {} -> {}", edge.getFromKey(), edge.getToKey());
          database.collection(EDGES_COLLECTION).updateDocument(edgeKey, edgeData);
        } else {
          throw e;
        }
      }
    } catch (Exception e) {
      log.error(
          "Failed to store edge: {} -> {}, skipping to prevent graph corruption",
          edge.getFromKey(),
          edge.getToKey(),
          e);
      // Don't throw - log and continue to prevent partial graph corruption
    }
  }

  /**
   * Store multiple edges in a batch operation.
   *
   * @param edges list of edges to store
   * @throws IoException if batch operation fails
   */
  @Override
  public void storeEdges(List<GraphEdge> edges) {
    if (!initialized || database == null) {
      log.warn("ArangoDB service not initialized, skipping batch edge storage");
      return;
    }

    log.debug("Storing {} edges in batch", edges.size());
    for (GraphEdge edge : edges) {
      storeEdge(edge);
    }
  }

  /**
   * Check if a document exists in a collection.
   *
   * @param collection the collection name
   * @param key the document key
   * @return true if the document exists, false otherwise
   */
  private boolean documentExists(String collection, String key) {
    try {
      return database.collection(collection).documentExists(key);
    } catch (Exception e) {
      log.debug("Error checking if document exists: {}/{}", collection, key, e);
      return false;
    }
  }

  /**
   * Get the collection name for a given node type.
   *
   * @param nodeType the node type
   * @return collection name
   */
  private String getCollectionForNodeType(String nodeType) {
    return switch (nodeType) {
      case "entity" -> ENTITIES_COLLECTION;
      case "operation" -> OPERATIONS_COLLECTION;
      case "example" -> EXAMPLES_COLLECTION;
      case "field" -> FIELDS_COLLECTION;
      case "documentation" -> DOCUMENTATIONS_COLLECTION;
      default -> throw new IllegalArgumentException("Unknown node type: " + nodeType);
    };
  }

  /**
   * Validate that a key matches the expected node key format. Valid formats: entity|..., op|...,
   * field|..., example|..., doc|...
   *
   * @param key the key to validate
   * @return true if valid, false otherwise
   */
  private boolean isValidNodeKey(String key) {
    if (key == null || key.isEmpty()) {
      return false;
    }
    // Check for valid prefixes (before sanitization)
    return key.startsWith("entity|")
        || key.startsWith("op|")
        || key.startsWith("field|")
        || key.startsWith("example|")
        || key.startsWith("doc|")
        ||
        // Also check sanitized format (after | -> _ replacement)
        key.startsWith("entity_")
        || key.startsWith("op_")
        || key.startsWith("field_")
        || key.startsWith("example_")
        || key.startsWith("doc_");
  }

  /**
   * Determine the collection from a node key prefix. Handles both original (with |) and sanitized
   * (with _) key formats.
   *
   * @param key the node key (may be original or sanitized)
   * @return collection name
   */
  private String determineCollectionFromKey(String key) {
    // Handle both formats: entity|... and entity_... (after sanitization)
    if (key.startsWith("entity|") || key.startsWith("entity_")) return ENTITIES_COLLECTION;
    if (key.startsWith("op|") || key.startsWith("op_")) return OPERATIONS_COLLECTION;
    if (key.startsWith("example|") || key.startsWith("example_")) return EXAMPLES_COLLECTION;
    if (key.startsWith("field|") || key.startsWith("field_")) return FIELDS_COLLECTION;
    if (key.startsWith("doc|") || key.startsWith("doc_")) return DOCUMENTATIONS_COLLECTION;

    // Log warning for unknown key format
    log.warn(
        "Could not determine collection for key: {}, defaulting to {}", key, ENTITIES_COLLECTION);
    return ENTITIES_COLLECTION;
  }

  /**
   * Sanitize a key to be valid for ArangoDB document keys. ArangoDB allows: a-z, A-Z, 0-9, _, -,
   * :, @, . This method converts our internal format (with | and <>) to ArangoDB-compatible format.
   *
   * @param key the key to sanitize (may contain | and <>)
   * @return sanitized key compatible with ArangoDB
   */
  private String sanitizeKeyForArangoDB(String key) {
    if (key == null) return "";
    // Replace | with _ and <> with _to_
    String sanitized = key.replace("|", "_").replace("<>", "_to_");
    // Remove any other invalid characters
    return sanitized.replaceAll("[^a-zA-Z0-9_\\-:@\\.]", "_");
  }

  /**
   * Sanitize a graph name to be valid for ArangoDB.
   *
   * @param name the graph name to sanitize
   * @return sanitized graph name
   */
  private String sanitizeGraphName(String name) {
    // ArangoDB graph names: alphanumeric, underscore, hyphen
    // Must start with letter
    String sanitized = name.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();

    // Ensure it starts with a letter
    if (!sanitized.matches("^[a-zA-Z].*")) {
      sanitized = "handbook_" + sanitized;
    }

    return sanitized;
  }

  /**
   * Clear all graph data from the handbook-specific database.
   *
   * <p>This method clears all collections and drops any existing graphs in the current handbook's
   * database to ensure a clean state for re-indexing. Only affects the database for this specific
   * handbook.
   *
   * @throws IoException if clear operation fails
   */
  @Override
  public void clearAllData() {
    if (!initialized || database == null) {
      log.warn("ArangoDB service not initialized, nothing to clear");
      return;
    }

    log.warn(
        "Clearing all graph data from ArangoDB database '{}' for handbook '{}'",
        databaseName,
        handbookName);
    try {
      // IMPORTANT: Drop all graphs FIRST before dropping collections
      // Collections that are part of a graph cannot be dropped directly
      try {
        Collection<com.arangodb.entity.GraphEntity> graphs = database.getGraphs();
        for (com.arangodb.entity.GraphEntity graphEntity : graphs) {
          String graphName = graphEntity.getName();
          log.info("Dropping graph: {}", graphName);
          database.graph(graphName).drop();
        }
      } catch (Exception e) {
        log.debug("Could not list or drop graphs (may not exist)", e);
      }

      // Now drop all collections (except system collections)
      Collection<com.arangodb.entity.CollectionEntity> collections = database.getCollections();
      for (com.arangodb.entity.CollectionEntity collectionEntity : collections) {
        String collectionName = collectionEntity.getName();

        // Skip system collections (they start with underscore)
        if (collectionName.startsWith("_")) {
          continue;
        }

        log.info("Dropping collection: {}", collectionName);
        try {
          database.collection(collectionName).drop();
        } catch (Exception e) {
          log.warn("Failed to drop collection: {}", collectionName, e);
        }
      }

      log.info("Successfully cleared all graph data and dropped existing graphs");

      // Recreate the collections we need
      log.info("Recreating collections after clearing");
      createCollectionIfNotExists(ENTITIES_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(OPERATIONS_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(EXAMPLES_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(FIELDS_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(DOCUMENTATIONS_COLLECTION, CollectionType.DOCUMENT);
      createCollectionIfNotExists(EDGES_COLLECTION, CollectionType.EDGES);

      log.info("Collections recreated and ready for indexing");
    } catch (Exception e) {
      throw new IoException("Failed to clear graph data", e);
    }
  }

  /** Ensure the named graph exists. Call this after indexing is complete. */
  @Override
  public void ensureGraphExists() {
    if (!initialized || database == null) {
      return;
    }
    createNamedGraph();
  }

  /** Close the ArangoDB connection and release resources. */
  @Override
  public void shutdown() {
    if (arangoDB != null) {
      try {
        arangoDB.shutdown();
        log.info("ArangoDB connection closed");
      } catch (Exception e) {
        log.warn("Error closing ArangoDB connection", e);
      }
    }
  }

  /**
   * Get the ArangoDB database instance.
   *
   * @return the ArangoDatabase instance
   */
  public ArangoDatabase getDatabase() {
    return database;
  }

  /**
   * Get the database name for this handbook.
   *
   * @return the database name (e.g., "onemcp_acme-handbook")
   */
  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Get the handbook name.
   *
   * @return the handbook name
   */
  @Override
  public String getHandbookName() {
    return handbookName;
  }

  /**
   * Check if the service is initialized and ready to use.
   *
   * @return true if initialized, false otherwise
   */
  public boolean isInitialized() {
    return initialized;
  }
}
