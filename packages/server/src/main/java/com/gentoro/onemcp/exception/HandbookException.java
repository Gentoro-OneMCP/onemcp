package com.gentoro.onemcp.exception;

/** Errors during knowledge base ingestion, indexing, or querying. */
public class HandbookException extends OneMcpException {
  public HandbookException(String message) {
    super(OneMcpErrorCode.HANDBOOK_ERROR, message);
  }

  public HandbookException(String message, Throwable cause) {
    super(OneMcpErrorCode.HANDBOOK_ERROR, message, cause);
  }
}
