package com.gentoro.onemcp.indexing.driver.spi;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphDriver;

/**
 * Service Provider Interface for pluggable GraphDriver backends.
 *
 * <p>Implementations must register using ServiceLoader by adding their fully qualified class name
 * to: META-INF/services/com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider
 */
public interface GraphDriverProvider {
  /** Unique driver id used in configuration, e.g., "in-memory", "arangodb", "neo4j". */
  String id();

  /**
   * Whether the provider can operate in the current runtime (e.g., dependencies present, enabled).
   */
  default boolean isAvailable(OneMcp oneMcp) {
    return true;
  }

  /** Create a new GraphDriver instance bound to this OneMcp and handbook name. */
  GraphDriver create(OneMcp oneMcp, String handbookName);
}
