package com.gentoro.onemcp.management.async;

import java.nio.file.Path;

/**
 * Immutable state holder for a handbook upload job.
 *
 * <p>Mutable fields {@code status} and {@code errorMessage} are intentionally left non-final but
 * are guaranteed safe because they are:
 *
 * <ul>
 *   <li>volatile for visibility guarantees
 *   <li>mutated only by the job executor thread
 * </ul>
 */
public final class HandbookJob {
  public final String jobId;
  public final Path tempDirectory;

  public volatile HandbookJobStatus status;
  public volatile String errorMessage;

  public HandbookJob(String jobId, Path tempDirectory) {
    this.jobId = jobId;
    this.tempDirectory = tempDirectory;
    this.status = HandbookJobStatus.PENDING;
  }
}
