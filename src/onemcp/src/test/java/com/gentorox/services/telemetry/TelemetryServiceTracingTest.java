package com.gentorox.services.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.gentorox.services.telemetry.TelemetryConstants.ATTR_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TelemetryServiceTracingTest {
  private InMemorySpanExporter spanExporter;
  private SdkTracerProvider tracerProvider;
  private OpenTelemetry openTelemetry;
  private TelemetryService telemetryService;

  @BeforeEach
  void setUp() {
    spanExporter = InMemorySpanExporter.create();
    tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
        .setResource(Resource.getDefault())
        .build();
    openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
    telemetryService = new TelemetryService(openTelemetry);
  }

  @AfterEach
  void tearDown() {
    tracerProvider.close();
  }

  @Test
  void runRoot_createsServerSpan_withAttributes() {
    var session = new TelemetrySession("abc-123");

    String result = telemetryService.runRoot(session, "orchestrate", Map.of("foo", "bar"), () -> "ok");

    assertThat(result).isEqualTo("ok");

    var spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).hasSize(1);
    var span = spans.getFirst();
    assertThat(span.getName()).isEqualTo("orchestrate");
    assertThat(span.getKind()).isEqualTo(SpanKind.SERVER);
    assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(ATTR_SESSION_ID)))
        .isEqualTo("abc-123");
    assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("foo")))
        .isEqualTo("bar");
  }

  @Test
  void inSpan_createsInternalSpan_andPropagatesParent() {
    var session = new TelemetrySession("s-1");
    telemetryService.runRoot(session, "root", () -> {
      telemetryService.inSpan("child", Map.of("k","v"), () -> "x");
      return null;
    });

    List varSpans = spanExporter.getFinishedSpanItems();
    assertThat(varSpans).hasSize(2);
    var child = spanExporter.getFinishedSpanItems().stream().filter(s -> s.getName().equals("child")).findFirst().orElseThrow();
    var root = spanExporter.getFinishedSpanItems().stream().filter(s -> s.getName().equals("root")).findFirst().orElseThrow();

    assertThat(child.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(child.getParentSpanId()).isEqualTo(root.getSpanContext().getSpanId());
  }

  @Test
  void exceptionsAreRecordedAndRethrown() {
    var session = new TelemetrySession("s-err");
    assertThatThrownBy(() -> telemetryService.runRoot(session, "work", () -> { throw new IllegalStateException("boom"); }))
        .isInstanceOf(IllegalStateException.class);

    var span = spanExporter.getFinishedSpanItems().getFirst();
    assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(span.getEvents()).anyMatch(e -> e.getName().equals("exception"));
  }
}
