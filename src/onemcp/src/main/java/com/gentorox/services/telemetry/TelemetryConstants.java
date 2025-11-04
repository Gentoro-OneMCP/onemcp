package com.gentorox.services.telemetry;

/**
 * Centralized telemetry constants for tracer/meter names and attribute keys.
 */
public final class TelemetryConstants {
  private TelemetryConstants() {}

  /** Tracer name used for manual spans. */
  public static final String TRACER = "com.gentorox.onemcp";
  /** Meter name used for custom application metrics. */
  public static final String METER  = "com.gentorox.onemcp";

  /** MDC key used to propagate the logical session identifier in logs. */
  public static final String MDC_SESSION_ID = "sessionId";

  /** Attribute keys used across spans/metrics. */
  public static final String ATTR_SESSION_ID = "gentorox.session.id";
  public static final String ATTR_PROVIDER   = "gentorox.model.provider";
  public static final String ATTR_MODEL      = "gentorox.model.name";
  public static final String ATTR_TOOL       = "gentorox.tool.name";
}
