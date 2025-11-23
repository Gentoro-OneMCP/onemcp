package com.gentoro.onemcp.engine;

import com.gentoro.onemcp.exception.OneMcpErrorCode;
import com.gentoro.onemcp.exception.OneMcpException;

/**
 * Execution-plan specific {@link OneMcpException}.
 *
 * <p>This exception is thrown whenever any part of the execution-plan lifecycle fails, including
 * but not limited to:
 *
 * <ul>
 *   <li>Structural validation problems detected by {@link ExecutionPlanValidator}.
 *   <li>Failures while resolving JsonPath expressions via {@link JsonPathResolver}.
 *   <li>Errors produced while evaluating conditions or expressions during plan execution.
 *   <li>Invalid routing or node configuration discovered by {@link ExecutionPlanEngine}.
 *   <li>Exceptions thrown by user‑provided operations invoked through {@link OperationRegistry}.
 * </ul>
 *
 * <p>The exception is consistently reported with {@link OneMcpErrorCode#EXECUTION_ERROR}, so it
 * integrates with the global OneMcp error-handling pipeline.
 */
public class ExecutionPlanException extends OneMcpException {

  public ExecutionPlanException(String message) {
    super(OneMcpErrorCode.EXECUTION_ERROR, message);
  }

  public ExecutionPlanException(String message, Throwable cause) {
    super(OneMcpErrorCode.EXECUTION_ERROR, message, cause);
  }
}
