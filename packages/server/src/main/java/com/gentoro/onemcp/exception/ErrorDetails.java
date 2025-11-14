package com.gentoro.onemcp.exception;

import java.time.Instant;
import java.util.Map;

/** Lightweight DTO to expose structured error information to logs or HTTP responses. */
public final class ErrorDetails {
  public final String type;
  public final String message;
  public final OneMcpErrorCode code;
  public final Map<String, Object> context;
  public final Instant timestamp;

  public ErrorDetails(
      String type,
      String message,
      OneMcpErrorCode code,
      Map<String, Object> context,
      Instant timestamp) {
    this.type = type;
    this.message = message;
    this.code = code;
    this.context = context;
    this.timestamp = timestamp;
  }
}
