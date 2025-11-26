package com.gentoro.onemcp.indexing;

import java.util.List;
import java.util.Map;

/**
 * Unified graph driver for storing and retrieving handbook-derived knowledge nodes.
 *
 * <p>This replaces the split Indexing/Query driver model with a single API that can: -
 * initialize/teardown the backend - clear or delete by handbook - upsert nodes in batches -
 * retrieve nodes by (Entity, Operation) context
 *
 * <p>Implementations can target different backends (e.g., Neo4j, ArangoDB, or an in-memory map)
 * without leaking vendor specifics to higher layers.
 */
public interface GraphDriver extends AutoCloseable {
  void initialize();

  boolean isInitialized();

  /** Clears all data for the current handbook/database (if applicable). */
  void clearAll();

  /** Persist or update a batch of nodes. */
  void upsertNodes(List<GraphNodeRecord> nodes);

  /**
   * Retrieve all nodes that match at least one provided tuple (Entity, Operation). If a node is
   * linked only to an entity (no operation), it should be returned as long as the entity matches
   * any tuple entity.
   *
   * @param contextTuples list of tuples with entity and zero-or-more operations
   * @return list of nodes (opaque maps) preserving node attributes
   */
  List<Map<String, Object>> queryByContext(List<GraphContextTuple> contextTuples);

  /** Optional targeted delete by node keys. */
  default void deleteNodesByKeys(List<String> keys) {}

  /** Logical backend/driver name. */
  String getDriverName();

  /** Handbook name or logical dataset identifier, when applicable. */
  String getHandbookName();

  void shutdown();

  @Override
  default void close() {
    shutdown();
  }
}
