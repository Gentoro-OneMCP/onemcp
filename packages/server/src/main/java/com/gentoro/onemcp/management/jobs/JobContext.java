package com.gentoro.onemcp.management.jobs;

import com.gentoro.onemcp.OneMcp;

/** Context passed to {@link JobHandler} with access to core services and cancellation flags. */
public final class JobContext {
  private final OneMcp oneMcp;
  private final String jobId;
  private final JobType jobType;
  private final CancelChecker cancelChecker;

  /** Functional interface checked by handlers to cooperatively cancel execution. */
  @FunctionalInterface
  public interface CancelChecker {
    boolean isCancelled();
  }

  public JobContext(OneMcp oneMcp, String jobId, JobType jobType, CancelChecker cancelChecker) {
    this.oneMcp = oneMcp;
    this.jobId = jobId;
    this.jobType = jobType;
    this.cancelChecker = cancelChecker;
  }

  public OneMcp oneMcp() {
    return oneMcp;
  }

  public String jobId() {
    return jobId;
  }

  public JobType jobType() {
    return jobType;
  }

  public boolean isCancelled() {
    return cancelChecker != null && cancelChecker.isCancelled();
  }
}
