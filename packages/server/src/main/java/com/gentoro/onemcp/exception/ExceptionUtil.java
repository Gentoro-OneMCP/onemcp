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

  /**
   * Extract just the error message from a throwable, without any stack trace information.
   * This is useful for user-facing error messages where stack traces are not helpful.
   * 
   * @param t the throwable to extract the message from
   * @return the error message, or a default message if none is available
   */
  public static String extractErrorMessage(Throwable t) {
    if (t == null) {
      return "Unknown error";
    }
    
    // First, recursively search the exception chain for API error messages
    // This is the most important information for the user
    Throwable current = t;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && !message.trim().isEmpty()) {
        // If we find an API error message anywhere in the chain, use it
        if (message.contains("API error:")) {
          int apiErrorIndex = message.indexOf("API error:");
          String apiError = message.substring(apiErrorIndex);
          // Clean up the error message - remove HTTP status prefix if present
          // Format: "API error: HTTP 400: Missing required fields..."
          // We want: "Missing required fields..."
          if (apiError.startsWith("API error: HTTP ")) {
            int colonIndex = apiError.indexOf(':', "API error: HTTP ".length());
            if (colonIndex > 0 && colonIndex < apiError.length() - 1) {
              return apiError.substring(colonIndex + 1).trim();
            }
          } else if (apiError.startsWith("API error: ")) {
            return apiError.substring("API error: ".length());
          }
          return apiError;
        }
        // Also check for HTTP error patterns
        if (message.contains("HTTP ") && (message.contains("4") || message.contains("5"))) {
          // Extract the error message after HTTP status
          // Format: "API error: HTTP 400: Missing required fields..."
          if (message.contains("API error: HTTP ")) {
            int httpIndex = message.indexOf("HTTP ");
            int colonAfterHttp = message.indexOf(':', httpIndex);
            if (colonAfterHttp > 0 && colonAfterHttp < message.length() - 1) {
              return message.substring(colonAfterHttp + 1).trim();
            }
          }
        }
      }
      current = current.getCause();
    }
    
    // If no API error found, extract a clean message from the top-level exception
    String className = t.getClass().getSimpleName();
    String message = t.getMessage();
    
    if (message != null && !message.trim().isEmpty()) {
      // If message contains stack trace info (e.g., from formatCompactStackTrace),
      // try to extract just the first meaningful part
      if (message.contains(" > ") || (message.contains("(") && message.contains(".java"))) {
        // This looks like a compact stack trace, just return the exception type
        return className;
      }
      
      // Check if message is just a stack trace (starts with class name and contains file info)
      if (message.contains(className) && (message.contains(".java:") || message.contains("("))) {
        // Extract just the part before any stack trace indicators
        int stackStart = message.indexOf("(");
        if (stackStart > 0) {
          String beforeStack = message.substring(0, stackStart).trim();
          // If there's a colon, try to get the message part
          int colonIndex = beforeStack.indexOf(':');
          if (colonIndex > 0 && colonIndex < beforeStack.length() - 1) {
            String msgPart = beforeStack.substring(colonIndex + 1).trim();
            if (!msgPart.isEmpty() && !msgPart.contains("(")) {
              return className + ": " + msgPart;
            }
          }
        }
        // If we can't extract a clean message, just return the exception type
        return className;
      }
      
      // Clean message, use it
      return className + ": " + message;
    }
    
    // If no message, return the exception class name
    return className;
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
