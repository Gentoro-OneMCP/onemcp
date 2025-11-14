package com.gentoro.onemcp.exception;

import java.time.Instant;
import java.util.function.Function;

/** Utility helpers for dealing with exceptions and structured error details. */
public final class ExceptionUtil {
  private ExceptionUtil() {}

  /**
   * Convert any {@link Throwable} into {@link ErrorDetails} for logging or API responses. If the
   * throwable is a {@link OneMcpException}, its code and context are preserved.
   */
  public static ErrorDetails toErrorDetails(Throwable t) {
    if (t instanceof OneMcpException ex) {
      return new ErrorDetails(
          ex.getClass().getSimpleName(),
          safeMessage(ex.getMessage()),
          ex.getCode(),
          ex.getContext(),
          Instant.now());
    }
    return new ErrorDetails(
        t.getClass().getSimpleName(),
        safeMessage(t.getMessage()),
        OneMcpErrorCode.UNKNOWN,
        null,
        Instant.now());
  }

  /**
   * Produce a compact, human-friendly representation of a throwable's stack trace. It captures only
   * the first line of each stack frame up to the provided limit and joins them in call-order using
   * a friendly separator.
   *
   * <p>Example output: {@code com.example.Foo.bar(Foo.java:42) > com.example.App.main(App.java:10)}
   *
   * @param t the throwable whose stack should be summarized (null returns empty string)
   * @param maxFrames maximum number of top stack frames to include; if <= 0, includes all frames
   * @return a single-line compact stack trace string
   */
  public static String formatCompactStackTrace(Throwable t, int maxFrames) {
    if (t == null) return "";
    StackTraceElement[] elements = t.getStackTrace();
    if (elements == null || elements.length == 0) return "";

    int limit = maxFrames <= 0 ? elements.length : Math.min(elements.length, maxFrames);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < limit; i++) {
      StackTraceElement e = elements[i];
      // Format: class.method (File:line) — mirrors the first line style but cleaner
      sb.append(e.getClassName())
          .append('.')
          .append(e.getMethodName())
          .append(" (")
          .append(e.getFileName() == null ? "Unknown Source" : e.getFileName());
      if (e.getLineNumber() >= 0) {
        sb.append(':').append(e.getLineNumber());
      }
      sb.append(')');
      if (i < limit - 1) sb.append(" > ");
    }
    return sb.toString();
  }

  /** Convenience overload using a reasonable default of 10 frames. */
  public static String formatCompactStackTrace(Throwable t) {
    return formatCompactStackTrace(t, 10);
  }

  private static String safeMessage(String message) {
    return message == null ? "" : message;
  }

  public static OneMcpException rethrowIfUnchecked(
      Throwable t, Function<Throwable, OneMcpException> supplier) {
    if (t instanceof OneMcpException) {
      return (OneMcpException) t;
    } else {
      return supplier.apply(t);
    }
  }
}
