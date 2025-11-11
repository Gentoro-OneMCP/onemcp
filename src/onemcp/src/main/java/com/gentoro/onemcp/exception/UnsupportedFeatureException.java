package com.gentoro.onemcp.exception;

/** Feature or capability is not supported by the current provider or configuration. */
public class UnsupportedFeatureException extends OneMcpException {
  public UnsupportedFeatureException(String message) {
    super(OneMcpErrorCode.UNSUPPORTED_FEATURE, message);
  }

  public UnsupportedFeatureException(String message, Throwable cause) {
    super(OneMcpErrorCode.UNSUPPORTED_FEATURE, message, cause);
  }
}
