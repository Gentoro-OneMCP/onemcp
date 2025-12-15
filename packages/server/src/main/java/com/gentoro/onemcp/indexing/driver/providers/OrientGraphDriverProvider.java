package com.gentoro.onemcp.indexing.driver.providers;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.driver.orient.OrientGraphDriver;
import com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider;

/** Service provider for OrientDB-based GraphDriver. */
public class OrientGraphDriverProvider implements GraphDriverProvider {
  @Override
  public String id() {
    return "orientdb";
  }

  @Override
  public boolean isAvailable(OneMcp oneMcp) {
    String desired = oneMcp.configuration().getString("indexing.graph.driver", "in-memory");
    if ("orientdb".equalsIgnoreCase(desired)) return true;

    // Presence of orient config can also signal availability
    String root = oneMcp.configuration().getString("indexing.graph.orient.rootDir", null);
    return root != null;
  }

  @Override
  public GraphDriver create(OneMcp oneMcp, String handbookName) {
    return new OrientGraphDriver(oneMcp, handbookName);
  }
}
