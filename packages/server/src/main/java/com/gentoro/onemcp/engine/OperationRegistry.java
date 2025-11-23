package com.gentoro.onemcp.engine;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Registry of named operations that can be invoked by {@link ExecutionPlanEngine}.
 *
 * <p>The registry is intentionally minimal: it stores functions from {@link JsonNode} to {@link
 * JsonNode}. The engine uses it to look up the implementation referenced by the {@code operation}
 * field of {@code operation_call} and {@code iterate} nodes.
 *
 * <p>The typical lifecycle is:
 *
 * <ol>
 *   <li>Create a registry.
 *   <li>Register one or more domain‑specific operations.
 *   <li>Pass the registry to {@link ExecutionPlanEngine}.
 * </ol>
 */
public class OperationRegistry {

  /** Backing map of operation name to implementation. */
  private final Map<String, Function<JsonNode, JsonNode>> operations = new HashMap<>();

  /**
   * Register a new operation.
   *
   * @param name symbolic name used from execution plans (via the {@code operation} field).
   * @param op function that implements the operation; receives the resolved input object and
   *     returns a JSON result (or {@code null}).
   * @return this registry for fluent usage.
   */
  public OperationRegistry register(String name, Function<JsonNode, JsonNode> op) {
    operations.put(name, op);
    return this;
  }

  /**
   * Invoke a previously registered operation.
   *
   * @param name operation name as referenced from a plan.
   * @param input input payload prepared by the engine for the operation.
   * @return the {@link JsonNode} result returned by the operation (may be {@code null}).
   * @throws ExecutionPlanException if the operation is not registered or if the implementation
   *     throws any exception.
   */
  public JsonNode invoke(String name, JsonNode input) {
    Function<JsonNode, JsonNode> op = operations.get(name);
    if (op == null) {
      throw new ExecutionPlanException("No operation registered with name '" + name + "'");
    }
    try {
      return op.apply(input);
    } catch (Exception e) {
      throw new ExecutionPlanException("Operation '" + name + "' failed", e);
    }
  }
}
