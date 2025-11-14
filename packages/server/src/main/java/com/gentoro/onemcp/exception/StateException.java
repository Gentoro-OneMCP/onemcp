package com.gentoro.onemcp.exception;

/** Illegal or unexpected state encountered. */
public class StateException extends OneMcpException {
  public StateException(String message) {
    super(OneMcpErrorCode.FAILED_PRECONDITION, message);
  }

  public StateException(String message, Throwable cause) {
    super(OneMcpErrorCode.FAILED_PRECONDITION, message, cause);
  }
}
