package com.gentorox.services.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenTelemetry configuration.
 *
 * <p>This configuration creates and wires an {@link OpenTelemetry} instance backed by
 * OTLP gRPC exporters for both traces and metrics. It follows current OpenTelemetry
 * recommendations for resource attributes and provides sane defaults for local
 * development.
 *
 * <p>Configuration precedence for the OTLP endpoint (first non-blank wins):
 * <ol>
 *   <li>Spring property: {@code otel.exporter.otlp.endpoint}</li>
 *   <li>Environment variable: {@code OTEL_EXPORTER_OTLP_ENDPOINT}</li>
 *   <li>Default: {@code http://localhost:4317}</li>
 * </ol>
 *
 * <p>Service identity is propagated using semantic resource attributes
 * {@code service.name} and {@code service.version}.
 */
@Configuration
public class TelemetryConfig {

  /**
   * Creates the Resource describing this service (name and version), merged with the
   * default SDK resource (host, OS, etc.).
   */
  @Bean
  public Resource otelResource(@Value("${otel.service.name:onemcp}") String serviceName) {
    return Resource.getDefault().merge(Resource.create(
        Attributes.builder()
            .put("service.name", serviceName)
            .put("service.version", System.getenv().getOrDefault("BUILD_VERSION", "dev"))
            .build()));
  }

  /**
   * Constructs a {@link SdkTracerProvider} with a batch span processor exporting via OTLP gRPC.
   * The bean is managed by Spring and will be closed automatically on context shutdown.
   */
  @Bean(destroyMethod = "close")
  public SdkTracerProvider sdkTracerProvider(
      Resource otelResource,
      @Value("${otel.exporter.otlp.endpoint:http://localhost:4317}") String otlpEndpoint) {

    var spanExporter = OtlpGrpcSpanExporter.builder().setEndpoint(otlpEndpoint).build();
    return SdkTracerProvider.builder()
        .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
        .setResource(otelResource)
        .build();
  }

  /**
   * Constructs a {@link SdkMeterProvider} with a periodic reader exporting via OTLP gRPC.
   * The bean is managed by Spring and will be closed automatically on context shutdown.
   */
  @Bean(destroyMethod = "close")
  public SdkMeterProvider sdkMeterProvider(
      Resource otelResource,
      @Value("${otel.exporter.otlp.endpoint:http://localhost:4317}") String otlpEndpoint) {

    var metricExporter = OtlpGrpcMetricExporter.builder().setEndpoint(otlpEndpoint).build();
    return SdkMeterProvider.builder()
        .setResource(otelResource)
        .registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
        .build();
  }

  /**
   * Exposes the main {@link OpenTelemetry} bean wired with the tracer and meter providers.
   */
  @Bean
  public OpenTelemetry openTelemetry(SdkTracerProvider sdkTracerProvider,
                                     SdkMeterProvider sdkMeterProvider) {
    return OpenTelemetrySdk.builder()
        .setTracerProvider(sdkTracerProvider)
        .setMeterProvider(sdkMeterProvider)
        .build();
  }
}
