package com.gentoro.onemcp.indexing.driver.providers;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.driver.neo4j.Neo4jGraphDriver;
import com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider;

/** Service provider for Neo4j-based GraphDriver (Bolt driver). */
public class Neo4jGraphDriverProvider implements GraphDriverProvider {
  @Override
  public String id() {
    return "neo4j";
  }

  @Override
  public boolean isAvailable(OneMcp oneMcp) {
    String desired = oneMcp.configuration().getString("graph.driver", "in-memory");
    return "neo4j".equalsIgnoreCase(desired);
  }

  @Override
  public GraphDriver create(OneMcp oneMcp, String handbookName) {
    return new Neo4jGraphDriver(oneMcp, handbookName);
  }
}
