package com.gentoro.onemcp.exception;

/** Errors raised while interacting with an LLM provider or interpreting its responses. */
public class LlmException extends OneMcpException {
  public LlmException(String message) {
    super(OneMcpErrorCode.LLM_ERROR, message);
  }

  public LlmException(String message, Throwable cause) {
    super(OneMcpErrorCode.LLM_ERROR, message, cause);
  }
}
