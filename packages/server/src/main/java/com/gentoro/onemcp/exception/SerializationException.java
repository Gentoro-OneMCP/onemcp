package com.gentoro.onemcp.exception;

/** JSON/YAML/XML serialization or deserialization error. */
public class SerializationException extends OneMcpException {
  public SerializationException(String message) {
    super(OneMcpErrorCode.SERIALIZATION_ERROR, message);
  }

  public SerializationException(String message, Throwable cause) {
    super(OneMcpErrorCode.SERIALIZATION_ERROR, message, cause);
  }
}
