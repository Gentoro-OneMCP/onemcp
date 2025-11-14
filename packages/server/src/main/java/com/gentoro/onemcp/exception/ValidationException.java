package com.gentoro.onemcp.exception;

/** Input validation failure or illegal argument. */
public class ValidationException extends OneMcpException {
  public ValidationException(String message) {
    super(OneMcpErrorCode.INVALID_ARGUMENT, message);
  }

  public ValidationException(String message, Throwable cause) {
    super(OneMcpErrorCode.INVALID_ARGUMENT, message, cause);
  }
}
