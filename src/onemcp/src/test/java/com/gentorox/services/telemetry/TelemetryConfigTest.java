package com.gentorox.services.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {TelemetryConfig.class})
@TestPropertySource(properties = {
    "otel.service.name=test-service",
    // ensure property takes precedence over env; we don't set env here
    "otel.exporter.otlp.endpoint=http://example-collector:4317"
})
class TelemetryConfigTest {

  @Autowired Resource resource;
  @Autowired SdkTracerProvider tracerProvider;
  @Autowired SdkMeterProvider meterProvider;
  @Autowired OpenTelemetry openTelemetry;

  @Test
  void beansArePresentAndResourceHasServiceAttributes() {
    assertThat(resource).isNotNull();
    assertThat(tracerProvider).isNotNull();
    assertThat(meterProvider).isNotNull();
    assertThat(openTelemetry).isNotNull();

    // Resource attributes should contain service.name and service.version
    var attrs = resource.getAttributes();
    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("service.name")))
        .isEqualTo("test-service");

    // BUILD_VERSION may not be set in tests; default is "dev"
    assertThat(attrs.get(io.opentelemetry.api.common.AttributeKey.stringKey("service.version")))
        .isNotNull();
  }
}
