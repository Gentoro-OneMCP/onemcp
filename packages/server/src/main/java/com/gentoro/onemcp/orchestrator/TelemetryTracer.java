package com.gentoro.onemcp.orchestrator;

import com.gentoro.onemcp.messages.AssigmentResult;
import java.util.*;

/** Minimal in-memory tracer for building a nested trace tree and aggregating token usage. */
public class TelemetryTracer {
  public static final String STATUS_OK = "OK";
  public static final String STATUS_ERROR = "ERROR";

  public static class Span {
    final String id = UUID.randomUUID().toString();
    final String name;
    final long startMs;
    long endMs;
    long durationMs;
    String status = STATUS_OK;
    final Map<String, Object> attributes = new LinkedHashMap<>();
    final List<Span> children = new ArrayList<>();
    final Span parent;

    Span(String name, Span parent) {
      this.name = name;
      this.parent = parent;
      this.startMs = System.currentTimeMillis();
    }
  }

  private final Deque<Span> stack = new ArrayDeque<>();
  private final Span root;
  private long promptTokens = 0L;
  private long completionTokens = 0L;
  private long totalTokens = 0L;

  public TelemetryTracer() {
    this.root = new Span("orchestration", null);
    stack.push(root);
  }

  public Span current() {
    return stack.peek();
  }

  public Span startChild(String name) {
    Span parent = current();
    Span span = new Span(name, parent);
    if (parent != null) parent.children.add(span);
    stack.push(span);
    return span;
  }

  public void endCurrentOk(Map<String, Object> attrs) {
    endCurrent(STATUS_OK, attrs);
  }

  public void endCurrentError(Map<String, Object> attrs) {
    endCurrent(STATUS_ERROR, attrs);
  }

  private void endCurrent(String status, Map<String, Object> attrs) {
    // Defensive: avoid popping when stack is empty or only root remains
    Span span;
    if (stack.isEmpty()) {
      return; // nothing to end
    } else if (stack.size() <= 1) {
      // only root is present; don't pop it, but update fields for visibility
      span = stack.peek();
    } else {
      span = stack.pop();
    }
    span.endMs = System.currentTimeMillis();
    span.durationMs = span.endMs - span.startMs;
    span.status = status;
    if (attrs != null && !attrs.isEmpty()) span.attributes.putAll(attrs);
  }

  public void addUsage(Long prompt, Long completion, Long total) {
    if (prompt != null) this.promptTokens += prompt;
    if (completion != null) this.completionTokens += completion;
    if (total != null) this.totalTokens += total;
    // if total is null but both prompt and completion present, they are already accounted above
  }

  public long promptTokens() {
    return promptTokens;
  }

  public long completionTokens() {
    return completionTokens;
  }

  public long totalTokens() {
    // Prefer explicit total aggregation if present; otherwise derive from parts
    long derived = promptTokens + completionTokens;
    return Math.max(totalTokens, derived);
  }

  public AssigmentResult.Trace toTrace() {
    return toTrace(root);
  }

  private AssigmentResult.Trace toTrace(Span s) {
    List<AssigmentResult.Trace> childTraces = new ArrayList<>();
    for (Span c : s.children) childTraces.add(toTrace(c));
    return new AssigmentResult.Trace(
        s.id,
        s.name,
        s.startMs,
        s.endMs,
        s.durationMs,
        s.status,
        Collections.unmodifiableMap(s.attributes),
        Collections.unmodifiableList(childTraces));
  }
}
