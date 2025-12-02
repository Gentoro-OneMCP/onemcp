package com.gentoro.onemcp.management.async;

/** Lifecycle state of a handbook upload job executed asynchronously. */
public enum HandbookJobStatus {
  /** Job accepted but not yet executed. */
  PENDING,

  /** Job is currently being processed in the executor. */
  RUNNING,

  /** Job completed successfully. */
  DONE,

  /** Job failed permanently. See error message for details. */
  FAILED
}
