package com.gentoro.onemcp.orchestrator.progress;

/**
 * Simple time and delta-based rate limiter for progress updates.
 *
 * <p>Allows an event to pass if at least {@code minIntervalMs} elapsed since the last accepted
 * event or if the completed counter changed by at least {@code minDelta} units.
 */
public class ProgressRateLimiter {
  private final long minIntervalMs;
  private final long minDelta;

  private long lastAcceptedAt;
  private long lastCompleted;

  public ProgressRateLimiter(long minIntervalMs, long minDelta) {
    this.minIntervalMs = Math.max(0, minIntervalMs);
    this.minDelta = Math.max(0, minDelta);
    this.lastAcceptedAt = 0L;
    this.lastCompleted = Long.MIN_VALUE;
  }

  /** Return true if the event should be emitted given current time and completed units. */
  public synchronized boolean tryAcquire(long nowMs, long completed) {
    boolean intervalOk = (nowMs - lastAcceptedAt) >= minIntervalMs;
    boolean deltaOk =
        (lastCompleted == Long.MIN_VALUE) || (Math.abs(completed - lastCompleted) >= minDelta);
    if (intervalOk || deltaOk) {
      lastAcceptedAt = nowMs;
      lastCompleted = completed;
      return true;
    }
    return false;
  }
}
