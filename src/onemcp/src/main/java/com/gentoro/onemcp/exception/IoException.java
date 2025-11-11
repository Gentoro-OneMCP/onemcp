package com.gentoro.onemcp.exception;

/** I/O operation failed (filesystem, classpath, network streams). */
public class IoException extends OneMcpException {
  public IoException(String message) {
    super(OneMcpErrorCode.IO_ERROR, message);
  }

  public IoException(String message, Throwable cause) {
    super(OneMcpErrorCode.IO_ERROR, message, cause);
  }
}
