package com.gentoro.onemcp.management.jobs;

/** SPI implemented by job-specific executors. */
public interface JobHandler {
  /** Executes job logic and returns an optional result payload. */
  JobExecution execute(
      JobContext ctx,
      byte[] requestBytes,
      String requestContentType,
      ProgressReporter progressReporter)
      throws Exception;

  interface ProgressReporter {
    void reportProgress(long total, long current, String message);
  }
}
