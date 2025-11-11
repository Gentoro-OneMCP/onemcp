package com.gentoro.onemcp.exception;

/** Resource requested was not found. */
public class NotFoundException extends OneMcpException {
  public NotFoundException(String message) {
    super(OneMcpErrorCode.NOT_FOUND, message);
  }

  public NotFoundException(String message, Throwable cause) {
    super(OneMcpErrorCode.NOT_FOUND, message, cause);
  }
}
