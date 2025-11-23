package com.gentoro.onemcp.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Thin wrapper around Jayway JsonPath used by the execution‑plan engine.
 *
 * <p>The resolver is configured to work directly with Jackson {@link JsonNode} instances and
 * returns {@link JsonNode} results for all evaluations. Missing leaf values are mapped to JSON
 * {@code null} instead of throwing, which makes it convenient to use inside JsonLogic expressions.
 *
 * <p>All JsonPath evaluation errors are wrapped in {@link ExecutionPlanException} so callers do not
 * need to depend on JsonPath‑specific exception types.
 */
public class JsonPathResolver {

  /**
   * Legacy constructor kept for backward compatibility with existing tests/usages that expect a
   * JsonPathResolver(ObjectMapper) even though the mapper is no longer needed.
   */
  public JsonPathResolver(ObjectMapper mapper) {
    // The current implementation delegates to a custom JsonPath that operates directly
    // on JsonNode instances and does not require an ObjectMapper. We keep this constructor
    // so older code can still instantiate the resolver without changes.
  }

  /** Default constructor for typical usage. */
  public JsonPathResolver() {}

  /**
   * Evaluate the given JsonPath {@code path} against {@code root}.
   *
   * @param root the root JSON value used as evaluation context; typically an object containing a
   *     {@code state} property.
   * @param path the JsonPath expression to evaluate; must be a valid Jayway JsonPath string.
   * @return the resolved {@link JsonNode} value, or JSON {@code null} when the path points to a
   *     missing leaf.
   * @throws ExecutionPlanException if JsonPath parsing or evaluation fails for any reason.
   */
  public JsonNode read(JsonNode root, String path) {
    if (path == null || path.isBlank()) {
      throw new ExecutionPlanException("JsonPath expression must not be null or empty");
    }
    try {
      return JsonPath.read(root, path);
    } catch (Exception e) {
      throw new ExecutionPlanException("Failed to evaluate JsonPath '%s'".formatted(path), e);
    }
  }
}
