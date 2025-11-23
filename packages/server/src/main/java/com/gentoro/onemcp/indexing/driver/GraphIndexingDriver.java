package com.gentoro.onemcp.indexing.driver;

import com.gentoro.onemcp.indexing.graph.GraphEdge;
import com.gentoro.onemcp.indexing.graph.nodes.GraphNode;
import java.util.List;

/**
 * Abstraction for graph database operations required by the indexing pipeline.
 *
 * <p>Implementations should encapsulate the specifics of how nodes and edges are stored, cleared,
 * and managed so higher-level services can remain database-agnostic.
 */
public interface GraphIndexingDriver extends AutoCloseable {

  /** Initialize the driver and any underlying connections/resources. */
  void initialize();

  /**
   * @return true when the driver is ready to accept operations.
   */
  boolean isInitialized();

  /** Remove existing data, if supported, to allow clean re-indexing. */
  void clearAllData();

  /** Ensure any visualization graph constructs exist (no-op for unsupported engines). */
  void ensureGraphExists();

  /** Persist a single node. */
  void storeNode(GraphNode node);

  /** Persist a batch of nodes. */
  void storeNodes(List<GraphNode> nodes);

  /** Persist a single edge. */
  void storeEdge(GraphEdge edge);

  /** Persist a batch of edges. */
  void storeEdges(List<GraphEdge> edges);

  /** Clean up resources. Equivalent to {@link #shutdown()}. */
  @Override
  default void close() {
    shutdown();
  }

  /** Shut down the driver and release resources. */
  void shutdown();

  /**
   * @return logical database name used by this driver.
   */
  String getDatabaseName();

  /**
   * @return handbook name associated with this driver instance.
   */
  String getHandbookName();
}
