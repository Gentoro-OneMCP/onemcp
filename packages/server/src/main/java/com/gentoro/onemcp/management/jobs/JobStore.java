package com.gentoro.onemcp.management.jobs;

import java.util.Optional;

/** Abstraction for storing job metadata and payloads. */
public interface JobStore {
  void put(JobRecord record);

  Optional<JobRecord> get(String id);
}
