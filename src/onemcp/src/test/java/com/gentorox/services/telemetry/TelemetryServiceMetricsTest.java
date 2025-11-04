package com.gentorox.services.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.internal.view.StringPredicates;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.gentorox.services.telemetry.TelemetryConstants.ATTR_MODEL;
import static com.gentorox.services.telemetry.TelemetryConstants.ATTR_PROVIDER;
import static com.gentorox.services.telemetry.TelemetryConstants.ATTR_SESSION_ID;
import static com.gentorox.services.telemetry.TelemetryConstants.ATTR_TOOL;
import static org.assertj.core.api.Assertions.assertThat;

class TelemetryServiceMetricsTest {
  private InMemoryMetricReader metricReader;
  private SdkMeterProvider meterProvider;
  private OpenTelemetry openTelemetry;
  private TelemetryService telemetryService;

  @BeforeEach
  void setUp() {
    metricReader = InMemoryMetricReader.create();
    meterProvider = SdkMeterProvider.builder()
        .setResource(Resource.getDefault())
        .registerMetricReader(metricReader)
        .build();
    openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    telemetryService = new TelemetryService(openTelemetry);
  }

  @AfterEach
  void tearDown() {
    meterProvider.close();
  }

  @Test
  void countPrompt_emitsCounterWithAttributes() {
    telemetryService.countPrompt("openai", "gpt-4o");

    var metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      if (metric.getName().equals("com.gentorox.prompts.total")) {
        var data = metric.getLongSumData().getPoints();
        assertThat(data).anySatisfy(point -> {
          Attributes attrs = point.getAttributes();
          assertThat(attrs.get(AttributeKey.stringKey(ATTR_PROVIDER))).isEqualTo("openai");
          assertThat(attrs.get(AttributeKey.stringKey(ATTR_MODEL))).isEqualTo("gpt-4o");
          assertThat(point.getValue()).isEqualTo(1);
        });
      }
    });
  }

  @Test
  void countTool_emitsCounterWithAttributes() {
    telemetryService.countTool("search");

    var metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      if (metric.getName().equals("com.gentorox.tool.calls.total")) {
        var data = metric.getLongSumData().getPoints();
        assertThat(data).anySatisfy(point -> {
          Attributes attrs = point.getAttributes();
          assertThat(attrs.get(AttributeKey.stringKey(ATTR_TOOL))).isEqualTo("search");
          assertThat(point.getValue()).isEqualTo(1);
        });
      }
    });
  }

  @Test
  void countModelCall_emitsCounterWithAttributes() {
    telemetryService.countModelCall("anthropic", "claude-3");

    var metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      if (metric.getName().equals("com.gentorox.model.calls.total")) {
        var data = metric.getLongSumData().getPoints();
        assertThat(data).anySatisfy(point -> {
          Attributes attrs = point.getAttributes();
          assertThat(attrs.get(AttributeKey.stringKey(ATTR_PROVIDER))).isEqualTo("anthropic");
          assertThat(attrs.get(AttributeKey.stringKey(ATTR_MODEL))).isEqualTo("claude-3");
          assertThat(point.getValue()).isEqualTo(1);
        });
      }
    });
  }
}
