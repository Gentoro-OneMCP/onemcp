package com.gentoro.onemcp.exception;

/** Source or snippet compilation failed. */
public class CompilationException extends OneMcpException {
  public CompilationException(String message) {
    super(OneMcpErrorCode.COMPILATION_ERROR, message);
  }

  public CompilationException(String message, Throwable cause) {
    super(OneMcpErrorCode.COMPILATION_ERROR, message, cause);
  }
}
