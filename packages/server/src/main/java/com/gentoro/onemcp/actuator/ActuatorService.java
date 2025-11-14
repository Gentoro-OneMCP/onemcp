package com.gentoro.onemcp.actuator;

import com.gentoro.onemcp.OneMcp;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import org.eclipse.jetty.ee10.servlet.ServletHolder;

/**
 * Simple health check endpoint mimicking Spring Boot's actuator health endpoint.
 *
 * <p>Registers a servlet at path: /actuator/health
 *
 * <p>Response body:
 * {"status":"UP"}
 */
public class ActuatorService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ActuatorService.class);

  private final OneMcp oneMcp;

  public ActuatorService(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /** Register the actuator servlet with the shared Jetty context handler. */
  public void register() {
    oneMcp
        .httpServer()
        .getContextHandler()
        .addServlet(new ServletHolder(new ActuatorServlet()), "/actuator/health");
    log.info("Actuator health endpoint registered at /actuator/health");
  }

  private static class ActuatorServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setStatus(200);
      resp.setContentType("application/json");
      // Fixed response exactly as requested
      String payload = "{\"status\": \"UP\"}";
      try (PrintWriter out = resp.getWriter()) {
        out.println(payload);
      }
    }
  }
}
