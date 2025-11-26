package com.gentoro.onemcp.indexing.driver.providers;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.driver.memory.InMemoryGraphDriver;
import com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider;

public class InMemoryGraphDriverProvider implements GraphDriverProvider {
  @Override
  public String id() {
    return "in-memory";
  }

  @Override
  public GraphDriver create(OneMcp oneMcp, String handbookName) {
    return new InMemoryGraphDriver(handbookName);
  }
}
