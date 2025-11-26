package com.gentoro.onemcp.cache;

import com.gentoro.onemcp.exception.ExecutionException;

/**
 * Exception thrown when prompt normalization fails, potentially containing the raw JSON
 * that failed to parse.
 */
public class PromptNormalizationException extends ExecutionException {
  private final String rawJson;

  public PromptNormalizationException(String message, String rawJson) {
    super(message);
    this.rawJson = rawJson;
  }

  public PromptNormalizationException(String message, Throwable cause, String rawJson) {
    super(message, cause);
    this.rawJson = rawJson;
  }

  public String getRawJson() {
    return rawJson;
  }
}
