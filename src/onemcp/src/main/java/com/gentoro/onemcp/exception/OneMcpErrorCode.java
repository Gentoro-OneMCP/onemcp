package com.gentoro.onemcp.exception;

/**
 * Canonical error codes for OneMcp. Inspired by Google/RPC style codes and common OSS patterns.
 * Codes are stable and suitable for downstream services and logs. Prefer choosing the most specific
 * code that reflects the failure origin and actionability.
 */
public enum OneMcpErrorCode {
  // Generic
  UNKNOWN,
  INVALID_ARGUMENT,
  FAILED_PRECONDITION,
  NOT_FOUND,
  ALREADY_EXISTS,
  PERMISSION_DENIED,
  UNAUTHENTICATED,
  RESOURCE_EXHAUSTED,
  CANCELLED,
  ABORTED,

  // I/O and configuration
  CONFIGURATION_ERROR,
  IO_ERROR,
  SERIALIZATION_ERROR,

  // Domain specific
  COMPILATION_ERROR,
  EXECUTION_ERROR,
  PROMPT_ERROR,
  LLM_ERROR,
  KNOWLEDGE_BASE_ERROR,
  NETWORK_ERROR,
  UNSUPPORTED_FEATURE,
}
