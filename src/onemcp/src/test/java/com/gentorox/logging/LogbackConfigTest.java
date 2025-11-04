package com.gentorox.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.Appender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class LogbackConfigTest {
  @Test
  void otelAppenderIsPresentOnRootLogger() {
    LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
    Logger root = ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    Appender<?> otel = root.getAppender("OTEL");
    assertNotNull(otel, "Expected OTEL appender to be configured on root logger");
    String expected = "io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender";
    assertEquals(expected, otel.getClass().getName(), "OTEL appender is not the OpenTelemetryAppender v1_0");
  }

  @Test
  void consoleAppenderIsPresent() {
    LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
    Logger root = ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    Appender<?> console = root.getAppender("CONSOLE");
    assertNotNull(console, "Expected CONSOLE appender to be configured on root logger");
  }
}
