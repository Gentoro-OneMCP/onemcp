package com.gentoro.onemcp.indexing.driver.providers;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.driver.arangodb.ArangoGraphDriver;
import com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider;

/** Service provider for ArangoDB-based GraphDriver. */
public class ArangoGraphDriverProvider implements GraphDriverProvider {
  @Override
  public String id() {
    return "arangodb";
  }

  @Override
  public boolean isAvailable(OneMcp oneMcp) {
    // Available if explicitly selected or Arango config block seems present
    String desired = oneMcp.configuration().getString("indexing.graph.driver", "in-memory");
    if ("arangodb".equalsIgnoreCase(desired)) return true;
    // or presence of arango host config
    String host = oneMcp.configuration().getString("indexing.graph.arangodb.host", null);
    return host != null;
  }

  @Override
  public GraphDriver create(OneMcp oneMcp, String handbookName) {
    return new ArangoGraphDriver(oneMcp, handbookName);
  }
}
