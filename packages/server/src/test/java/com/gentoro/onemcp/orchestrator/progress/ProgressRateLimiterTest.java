package com.gentoro.onemcp.orchestrator.progress;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ProgressRateLimiterTest {

  @Test
  void allowsFirstEventAndDeltaBasedEvents() {
    ProgressRateLimiter limiter = new ProgressRateLimiter(300, 2);

    // t=0, first event must pass
    assertTrue(limiter.tryAcquire(0, 0));

    // within interval and below delta -> blocked
    assertFalse(limiter.tryAcquire(100, 1));

    // delta reached (completed changed by >=2) -> allowed
    assertTrue(limiter.tryAcquire(150, 2));

    // time passed >= minInterval -> allowed even without delta
    assertTrue(limiter.tryAcquire(500, 2));
  }
}
