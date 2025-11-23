package com.gentoro.onemcp.indexing.driver;

import java.util.List;
import java.util.Map;

/**
 * Abstraction for executing read/query operations against the knowledge graph.
 *
 * <p>Implementations encapsulate how graph data is retrieved so higher-level services remain
 * database-agnostic and can switch drivers via configuration.
 */
public interface GraphQueryDriver extends AutoCloseable {

  /** Initialize the driver and underlying connections/resources. */
  void initialize();

  /**
   * @return true when the driver is ready to accept query operations.
   */
  boolean isInitialized();

  /**
   * Execute a query for the provided entity/categorical context and return driver-specific
   * structured data that can be transformed into {@code QueryResult}.
   *
   * @param entityName target entity name
   * @param requestedCategories categories/operations to include (can be {@code null} or empty)
   * @return raw structured data, or {@code null} when no data found
   */
  Map<String, Object> queryContext(String entityName, List<String> requestedCategories);

  /**
   * @return logical driver identifier (e.g., {@code arangodb}).
   */
  String getDriverName();

  /**
   * @return handbook name associated with this driver instance (when applicable).
   */
  String getHandbookName();

  /** Shut down the driver and release resources. */
  void shutdown();

  @Override
  default void close() {
    shutdown();
  }
}
