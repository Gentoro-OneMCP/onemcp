package com.gentoro.onemcp.management.jobs;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Simple in-memory JobStore implementation. */
public final class InMemoryJobStore implements JobStore {
  private final Map<String, JobRecord> map = new ConcurrentHashMap<>();

  @Override
  public void put(JobRecord record) {
    map.put(record.id, record);
  }

  @Override
  public Optional<JobRecord> get(String id) {
    return Optional.ofNullable(map.get(id));
  }
}
