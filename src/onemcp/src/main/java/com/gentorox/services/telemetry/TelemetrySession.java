package com.gentorox.services.telemetry;

import java.util.UUID;

public record TelemetrySession(String id) {
  public static TelemetrySession create() { return new TelemetrySession(UUID.randomUUID().toString()); }
}
