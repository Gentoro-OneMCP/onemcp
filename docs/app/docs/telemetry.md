---
id: telemetry
title: Telemetry (OpenTelemetry) Configuration
sidebar_label: Telemetry
slug: /telemetry
---

Overview
- This project uses OpenTelemetry (OTel) to emit tracing and metrics over the OTLP gRPC protocol. Spring wires the OpenTelemetry SDK via the TelemetryConfig class.

What is provided
- OpenTelemetry bean: A singleton OpenTelemetry instance for the application.
- Tracer provider: Batch processing and OTLP gRPC export of spans.
- Meter provider: Periodic reader with OTLP gRPC export of metrics.
- Resource: Standard service.name and service.version attributes merged with default environment/system attributes.

Configuration
- Service name: Spring property otel.service.name (default: onemcp).
- OTLP endpoint (first non-blank wins):
  1. Spring property: otel.exporter.otlp.endpoint
  2. Environment variable: OTEL_EXPORTER_OTLP_ENDPOINT
  3. Default: http://localhost:4317
- Build version: Environment variable BUILD_VERSION (default: dev) used as service.version.

Examples
- application.yaml:
  otel:
    service:
      name: my-service
    exporter:
      otlp:
        endpoint: http://otel-collector.monitoring:4317
- Environment variables:
  export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
  export BUILD_VERSION=1.2.3

Local development
- If no endpoint is configured, the SDK defaults to http://localhost:4317.
- Run a local OpenTelemetry Collector or any compatible OTLP endpoint to receive data.

Extending instrumentation
- Tracing: Inject OpenTelemetry where needed and use it to acquire tracers.
  Example (Java):
    @Autowired OpenTelemetry otel;
    var tracer = otel.getTracer("com.example.component");
    var span = tracer.spanBuilder("operation").startSpan();
    try {
      // do work
    } finally {
      span.end();
    }
- Metrics: Use the MeterProvider from OpenTelemetry to create meters and instruments.
  Example (Java):
    var meter = otel.getMeter("com.example.component");
    var counter = meter.counterBuilder("processed_items").build();
    counter.add(1);

TelemetryService helpers
- TelemetryService centralizes common patterns for spans and counters.
- Tracing helpers:
  - runRoot(session, name, attrs, Supplier): creates a SERVER/root span and executes code within its scope.
  - inSpan(session, name, attrs, Supplier): creates an INTERNAL span under the current context.
  - Runnable overloads exist for both methods, and attrs can be omitted.
- Metrics helpers:
  - countPrompt(session, provider, model)
  - countTool(session, toolName)
  - countModelCall(session, provider, model)
- All helpers are null-safe for the session parameter and optional attributes.

Usage examples
- Run code in a root span:
  telemetryService.runRoot(session, "orchestrate", Map.of("request.id", reqId), () -> {
    // your logic
    return result;
  });
- Run code in a child span:
  telemetryService.inSpan(session, "call-model", () -> callModel());
- Count events:
  telemetryService.countPrompt(session, provider, model);
  telemetryService.countTool(session, toolName);
  telemetryService.countModelCall(session, provider, model);

Lifecycle and shutdown
- The tracer and meter providers are exposed as Spring beans and will be closed automatically when the application context shuts down, ensuring data is flushed.

Troubleshooting
- No data in backend: Verify the endpoint configuration and that your collector is reachable from the application.
- Connection refused: Ensure a collector is running at the configured endpoint (default is localhost:4317).
- Version mismatch: If you see exporter errors, confirm that the backend expects OTLP over gRPC and that ports are correct.

Contributing
- Code style: Keep configuration small, composable beans with clear names and extensive Javadoc where behavior may not be obvious.
- Tests: When adding new telemetry features, consider adding small integration tests that verify spans or metrics are produced under basic scenarios (using test exporters).
- Documentation: Update this page when introducing new configuration knobs or changing defaults.
