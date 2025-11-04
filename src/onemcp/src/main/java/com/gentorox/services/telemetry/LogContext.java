package com.gentorox.services.telemetry;

import org.slf4j.MDC;

/**
 * Simple MDC scope to propagate the current TelemetrySession id into logs.
 */
public final class LogContext implements AutoCloseable {
  public LogContext(TelemetrySession s) {
    if (s != null) MDC.put(TelemetryConstants.MDC_SESSION_ID, s.id());
  }
  @Override public void close() { MDC.remove(TelemetryConstants.MDC_SESSION_ID); }
}
