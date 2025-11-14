package com.gentoro.onemcp.exception;

/** Error while executing user code, tools, or runtime operations. */
public class ExecutionException extends OneMcpException {
  public ExecutionException(String message) {
    super(OneMcpErrorCode.EXECUTION_ERROR, message);
  }

  public ExecutionException(String message, Throwable cause) {
    super(OneMcpErrorCode.EXECUTION_ERROR, message, cause);
  }
}
