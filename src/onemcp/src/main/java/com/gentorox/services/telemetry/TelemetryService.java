package com.gentorox.services.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageEntryMetadata;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static com.gentorox.services.telemetry.TelemetryConstants.*;

@Component
public class TelemetryService {
  private final Tracer tracer;
  private final LongCounter promptsTotal;
  private final LongCounter toolCallsTotal;
  private final LongCounter modelCallsTotal;

  // Use the same key you already export as a span/metric attribute
  private static final String BAGGAGE_SESSION_ID = ATTR_SESSION_ID;

  public TelemetryService(OpenTelemetry openTelemetry) {
    this.tracer = openTelemetry.getTracer(TRACER);
    Meter meter = openTelemetry.meterBuilder(METER).build();
    this.promptsTotal = meter
        .counterBuilder("com.gentorox.prompts.total")
        .setDescription("Total prompts received")
        .build();
    this.toolCallsTotal = meter
        .counterBuilder("com.gentorox.tool.calls.total")
        .setDescription("Tool calls executed")
        .build();
    this.modelCallsTotal = meter
        .counterBuilder("com.gentorox.model.calls.total")
        .setDescription("Model calls executed")
        .build();
  }

  // ------------ Tracing (unchanged from earlier answer) ------------

  public <T> T runRoot(TelemetrySession session, String name, Map<String, String> attrs, Supplier<T> body) {
    Objects.requireNonNull(name, "span name");
    Objects.requireNonNull(body, "body");

    SpanBuilder spanBuilder = tracer.spanBuilder(name)
        .setSpanKind(SpanKind.SERVER)
        .setNoParent();

    applySpanAttributes(spanBuilder, session, attrs);

    Span span = spanBuilder.startSpan();
    try (Scope spanScope = span.makeCurrent();
         Scope baggageScope = makeSessionBaggageCurrent(session)) {
      return body.get();
    } catch (RuntimeException e) {
      span.recordException(e);
      span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
      throw e;
    } finally {
      span.end();
    }
  }

  public <T> T runRoot(TelemetrySession session, String name, Supplier<T> body) {
    return runRoot(session, name, Collections.emptyMap(), body);
  }

  public void runRoot(TelemetrySession session, String name, Map<String, String> attrs, Runnable body) {
    runRoot(session, name, attrs, () -> { body.run(); return null; });
  }

  public void runRoot(TelemetrySession session, String name, Runnable body) {
    runRoot(session, name, Collections.emptyMap(), body);
  }

  public <T> T inSpan(String name, Map<String, String> attrs, Supplier<T> body) {
    Objects.requireNonNull(name, "span name");
    Objects.requireNonNull(body, "body");

    SpanBuilder spanBuilder = tracer.spanBuilder(name).setSpanKind(SpanKind.INTERNAL);

    String sessionId = getSessionIdFromContext();
    if (sessionId != null) {
      spanBuilder.setAttribute(ATTR_SESSION_ID, sessionId);
    }
    if (attrs != null) {
      attrs.forEach((k, v) -> { if (k != null && v != null) spanBuilder.setAttribute(AttributeKey.stringKey(k), v); });
    }

    Span span = spanBuilder.startSpan();
    try (Scope ignored = span.makeCurrent()) {
      return body.get();
    } catch (RuntimeException e) {
      span.recordException(e);
      span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR);
      throw e;
    } finally {
      span.end();
    }
  }

  public <T> T inSpan(String name, Supplier<T> body) {
    return inSpan(name, Collections.emptyMap(), body);
  }

  public void inSpan(String name, Map<String, String> attrs, Runnable body) {
    inSpan(name, attrs, () -> { body.run(); return null; });
  }

  public void inSpan(String name, Runnable body) {
    inSpan(name, Collections.emptyMap(), body);
  }

  // Back-compat overloads that accept TelemetrySession
  public <T> T inSpan(TelemetrySession session, String name, Map<String, String> attrs, Supplier<T> body) {
    return inSpan(name, merge(attrs, session), body);
  }
  public <T> T inSpan(TelemetrySession session, String name, Supplier<T> body) {
    return inSpan(session, name, Collections.emptyMap(), body);
  }
  public void inSpan(TelemetrySession session, String name, Map<String, String> attrs, Runnable body) {
    inSpan(session, name, attrs, () -> { body.run(); return null; });
  }
  public void inSpan(TelemetrySession session, String name, Runnable body) {
    inSpan(session, name, Collections.emptyMap(), body);
  }

  // ------------ Metrics (context-inferred overloads) ------------

  /** New ergonomic overload: infers session from context baggage. */
  public void countPrompt(String provider, String model) {
    Attributes attributes = buildMetricAttributesFromContext(Map.of(
        ATTR_PROVIDER, provider,
        ATTR_MODEL, model
    ));
    promptsTotal.add(1, attributes);
  }

  /** New ergonomic overload: with extra attributes. */
  public void countPrompt(String provider, String model, Map<String, String> extraAttrs) {
    Map<String, String> attrs = new HashMap<>(extraAttrs == null ? Map.of() : extraAttrs);
    attrs.put(ATTR_PROVIDER, provider);
    attrs.put(ATTR_MODEL, model);
    promptsTotal.add(1, buildMetricAttributesFromContext(attrs));
  }

  public void countTool(String toolName) {
    Attributes attributes = buildMetricAttributesFromContext(Map.of(
        ATTR_TOOL, toolName
    ));
    toolCallsTotal.add(1, attributes);
  }

  public void countTool(String toolName, Map<String, String> extraAttrs) {
    Map<String, String> attrs = new HashMap<>(extraAttrs == null ? Map.of() : extraAttrs);
    attrs.put(ATTR_TOOL, toolName);
    toolCallsTotal.add(1, buildMetricAttributesFromContext(attrs));
  }

  public void countModelCall(String provider, String model) {
    Attributes attributes = buildMetricAttributesFromContext(Map.of(
        ATTR_PROVIDER, provider,
        ATTR_MODEL, model
    ));
    modelCallsTotal.add(1, attributes);
  }

  public void countModelCall(String provider, String model, Map<String, String> extraAttrs) {
    Map<String, String> attrs = new HashMap<>(extraAttrs == null ? Map.of() : extraAttrs);
    attrs.put(ATTR_PROVIDER, provider);
    attrs.put(ATTR_MODEL, model);
    modelCallsTotal.add(1, buildMetricAttributesFromContext(attrs));
  }

  // ------------ Internal helpers ------------

  private static void applySpanAttributes(SpanBuilder spanBuilder, TelemetrySession session, Map<String, String> attrs) {
    if (session != null && session.id() != null) {
      spanBuilder.setAttribute(ATTR_SESSION_ID, session.id());
    }
    if (attrs != null) {
      attrs.forEach((k, v) -> {
        if (k != null && v != null) {
          spanBuilder.setAttribute(AttributeKey.stringKey(k), v);
        }
      });
    }
  }

  private static Scope makeSessionBaggageCurrent(TelemetrySession session) {
    if (session != null && session.id() != null) {
      Baggage baggage = Baggage.current().toBuilder()
          .put(BAGGAGE_SESSION_ID, session.id(), BaggageEntryMetadata.empty())
          .build();
      return baggage.makeCurrent();
    }
    return () -> {}; // no-op scope
  }

  private static String getSessionIdFromContext() {
    String value = Baggage.current().getEntryValue(BAGGAGE_SESSION_ID);
    return (value == null || value.isEmpty()) ? null : value;
  }

  private static Map<String, String> merge(Map<String, String> attrs, TelemetrySession session) {
    if (session == null || session.id() == null) return attrs;
    Map<String, String> out = (attrs == null) ? new HashMap<>() : new HashMap<>(attrs);
    out.putIfAbsent(ATTR_SESSION_ID, session.id());
    return out;
  }

  /** Build metric attributes preferring explicit session, else context baggage. */
  private static Attributes buildMetricAttributes(TelemetrySession session, Map<String, String> attrs) {
    AttributesBuilder builder = Attributes.builder();

    String sessionId = (session != null ? session.id() : null);
    if (sessionId == null) sessionId = getSessionIdFromContext();
    if (sessionId != null) builder.put(AttributeKey.stringKey(ATTR_SESSION_ID), sessionId);

    if (attrs != null) {
      attrs.forEach((k, v) -> { if (k != null && v != null) builder.put(AttributeKey.stringKey(k), v); });
    }
    return builder.build();
  }

  /** Build metric attributes reading session only from context baggage. */
  private static Attributes buildMetricAttributesFromContext(Map<String, String> attrs) {
    AttributesBuilder builder = Attributes.builder();

    String sessionId = getSessionIdFromContext();
    if (sessionId != null) builder.put(AttributeKey.stringKey(ATTR_SESSION_ID), sessionId);

    if (attrs != null) {
      attrs.forEach((k, v) -> { if (k != null && v != null) builder.put(AttributeKey.stringKey(k), v); });
    }
    return builder.build();
  }
}
