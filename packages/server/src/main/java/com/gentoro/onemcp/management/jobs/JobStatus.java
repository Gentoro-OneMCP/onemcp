package com.gentoro.onemcp.management.jobs;

/** Generic lifecycle state for management jobs. */
public enum JobStatus {
  /** Job accepted but not yet executed. */
  PENDING,
  /** Job is currently executing. */
  RUNNING,
  /** Job finished successfully. */
  DONE,
  /** Job failed permanently. */
  FAILED,
  /** Job was cancelled by user request. */
  CANCELLED
}
