package com.gentoro.onemcp.messages;

import java.util.List;
import java.util.Map;

public record AssigmentResult(
    List<Assignment> parts, Statistics statistics, AssignmentContext context, String reportPath) {
  public record Assignment(
      boolean isSupported, String assignment, boolean isError, String content) {}

  /** Execution statistics enriched with token usage (provider-reported) and a nested trace tree. */
  public record Statistics(
      long promptTokens,
      long completionTokens,
      long totalTokens,
      long totalTimeMs,
      List<String> operations,
      Trace trace) {}

  /** Simple nested trace structure to represent orchestration spans. */
  public record Trace(
      String id,
      String name,
      long startMs,
      long endMs,
      long durationMs,
      String status,
      Map<String, Object> attributes,
      List<Trace> children) {}
}
