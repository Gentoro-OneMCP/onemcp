package com.gentoro.onemcp.exception;

/** Prompt retrieval, parsing, rendering, or repository initialization error. */
public class PromptException extends OneMcpException {
  public PromptException(String message) {
    super(OneMcpErrorCode.PROMPT_ERROR, message);
  }

  public PromptException(String message, Throwable cause) {
    super(OneMcpErrorCode.PROMPT_ERROR, message, cause);
  }
}
