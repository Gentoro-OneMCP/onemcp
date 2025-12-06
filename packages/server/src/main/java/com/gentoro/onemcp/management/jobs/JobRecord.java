package com.gentoro.onemcp.management.jobs;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Internal in-memory representation of a job and its data. This is intentionally package-private
 * and not exposed outside the jobs package.
 */
final class JobRecord {
  final String id;
  final JobType type;
  final Instant createdAt = Instant.now();

  volatile Instant startedAt;
  volatile Instant finishedAt;
  volatile JobStatus status = JobStatus.PENDING;
  volatile String message;

  // Request payload (when applicable)
  final byte[] requestBytes;
  final String requestContentType;
  final Map<String, String> meta;

  // Result payload (when applicable)
  volatile byte[] resultBytes;
  volatile String resultContentType;

  // Execution handle
  volatile Future<?> future;
  volatile boolean cancelRequested;

  JobRecord(
      String id,
      JobType type,
      byte[] requestBytes,
      String requestContentType,
      Map<String, String> meta) {
    this.id = id;
    this.type = type;
    this.requestBytes = requestBytes;
    this.requestContentType = requestContentType;
    this.meta = meta;
  }
}
