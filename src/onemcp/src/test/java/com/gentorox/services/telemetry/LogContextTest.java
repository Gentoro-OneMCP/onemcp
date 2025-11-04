package com.gentorox.services.telemetry;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static com.gentorox.services.telemetry.TelemetryConstants.MDC_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

class LogContextTest {

  @Test
  void putsAndRemovesSessionIdInMdc() {
    MDC.clear();
    var session = new TelemetrySession("sess-1");
    try (var ctx = new LogContext(session)) {
      assertThat(MDC.get(MDC_SESSION_ID)).isEqualTo("sess-1");
    }
    assertThat(MDC.get(MDC_SESSION_ID)).isNull();
  }

  @Test
  void nullSessionDoesNotThrowAndRemovesKeyOnClose() {
    MDC.put(MDC_SESSION_ID, "x");
    try (var ctx = new LogContext(null)) {
      // nothing added
    }
    assertThat(MDC.get(MDC_SESSION_ID)).isNull();
  }
}
