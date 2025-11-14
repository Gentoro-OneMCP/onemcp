package com.gentoro.onemcp.exception;

/** Errors during knowledge base ingestion, indexing, or querying. */
public class KnowledgeBaseException extends OneMcpException {
  public KnowledgeBaseException(String message) {
    super(OneMcpErrorCode.KNOWLEDGE_BASE_ERROR, message);
  }

  public KnowledgeBaseException(String message, Throwable cause) {
    super(OneMcpErrorCode.KNOWLEDGE_BASE_ERROR, message, cause);
  }
}
