package com.gentoro.onemcp.exception;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base runtime exception for OneMcp with a stable {@link OneMcpErrorCode} and optional context.
 *
 * <p>Design goals: - Immutable public surface (context map is defensively copied and unmodifiable)
 * - Human-friendly message, machine-friendly code - Easy to extend with specific exception
 * subclasses
 */
public class OneMcpException extends RuntimeException {
  private final OneMcpErrorCode code;
  private final Map<String, Object> context;

  public OneMcpException(OneMcpErrorCode code, String message) {
    super(message);
    this.code = Objects.requireNonNull(code, "code");
    this.context = Collections.emptyMap();
  }

  public OneMcpException(OneMcpErrorCode code, String message, Throwable cause) {
    super(message, cause);
    this.code = Objects.requireNonNull(code, "code");
    this.context = Collections.emptyMap();
  }

  public OneMcpException(OneMcpErrorCode code, String message, Map<String, ?> context) {
    super(message);
    this.code = Objects.requireNonNull(code, "code");
    this.context = copy(context);
  }

  public OneMcpException(
      OneMcpErrorCode code, String message, Map<String, ?> context, Throwable cause) {
    super(message, cause);
    this.code = Objects.requireNonNull(code, "code");
    this.context = copy(context);
  }

  public OneMcpErrorCode getCode() {
    return code;
  }

  /** Additional key/value details that help diagnosing the error. */
  public Map<String, Object> getContext() {
    return context;
  }

  private static Map<String, Object> copy(Map<String, ?> input) {
    if (input == null || input.isEmpty()) return Collections.emptyMap();
    Map<String, Object> m = new LinkedHashMap<>();
    input.forEach((k, v) -> m.put(k, v));
    return Collections.unmodifiableMap(m);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "{"
        + "code="
        + code
        + ", message="
        + String.valueOf(getMessage())
        + (context.isEmpty() ? "" : ", context=" + context)
        + (getCause() == null ? "" : ", cause=" + getCause().getClass().getSimpleName())
        + '}';
  }
}
