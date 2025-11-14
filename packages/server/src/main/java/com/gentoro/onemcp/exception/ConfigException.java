package com.gentoro.onemcp.exception;

/** Configuration or environment related problem detected at startup or runtime. */
public class ConfigException extends OneMcpException {
  public ConfigException(String message) {
    super(OneMcpErrorCode.CONFIGURATION_ERROR, message);
  }

  public ConfigException(String message, Throwable cause) {
    super(OneMcpErrorCode.CONFIGURATION_ERROR, message, cause);
  }
}
