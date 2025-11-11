package com.gentoro.onemcp.exception;

/** Network-level communication error (HTTP, sockets, timeouts). */
public class NetworkException extends OneMcpException {
  public NetworkException(String message) {
    super(OneMcpErrorCode.NETWORK_ERROR, message);
  }

  public NetworkException(String message, Throwable cause) {
    super(OneMcpErrorCode.NETWORK_ERROR, message, cause);
  }
}
