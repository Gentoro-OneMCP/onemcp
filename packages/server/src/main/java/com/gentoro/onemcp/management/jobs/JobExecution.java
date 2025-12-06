package com.gentoro.onemcp.management.jobs;

/** Result of a job execution. Contains an optional result payload and a final message. */
public final class JobExecution {
  public final byte[] resultBytes; // may be null
  public final String resultContentType; // may be null
  public final String message; // may be null

  public JobExecution(byte[] resultBytes, String resultContentType, String message) {
    this.resultBytes = resultBytes;
    this.resultContentType = resultContentType;
    this.message = message;
  }
}
