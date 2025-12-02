package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.management.async.HandbookJob;
import com.gentoro.onemcp.management.async.HandbookJobManager;
import jakarta.servlet.http.*;
import java.io.IOException;

/**
 * PUT /mng/handbook
 *
 * <p>Accepts a gzip-compressed tar archive of the handbook. Processing is asynchronous. Returns:
 *
 * <pre>
 * {
 *   "jobId": "...",
 *   "status": "accepted"
 * }
 * </pre>
 */
public final class HandbookUploadServlet extends HttpServlet {

  private HandbookJobManager jobManager;
  private final ObjectMapper mapper = new ObjectMapper();

  // No-arg constructor required by some servlet containers and tests
  public HandbookUploadServlet() {
    this.jobManager = null;
  }

  public HandbookUploadServlet(HandbookJobManager jobManager) {
    this.jobManager = jobManager;
  }

  @Override
  public void init(jakarta.servlet.ServletConfig config) throws jakarta.servlet.ServletException {
    super.init(config);
    if (this.jobManager == null && config != null) {
      var ctx = config.getServletContext();
      if (ctx != null) {
        // Try common attribute key
        Object candidate = ctx.getAttribute("jobManager");
        if (candidate instanceof HandbookJobManager m) {
          this.jobManager = m;
        } else {
          // As a fallback, scan for any attribute of expected type
          var names = ctx.getAttributeNames();
          while (names.hasMoreElements()) {
            String name = names.nextElement();
            Object val = ctx.getAttribute(name);
            if (val instanceof HandbookJobManager m2) {
              this.jobManager = m2;
              break;
            }
          }
        }
      }
    }
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
      // Support both constructor injection and ServletContext attribute for tests/runtime wiring
      HandbookJobManager jm;
      if (this.jobManager != null) {
        jm = this.jobManager;
      } else {
        var ctx = req.getServletContext() != null ? req.getServletContext() : getServletContext();
        jm = null;
        if (ctx != null) {
          Object candidate = ctx.getAttribute("jobManager");
          if (candidate instanceof HandbookJobManager m) {
            jm = m;
          } else {
            var names = ctx.getAttributeNames();
            while (names.hasMoreElements()) {
              String name = names.nextElement();
              Object val = ctx.getAttribute(name);
              if (val instanceof HandbookJobManager m2) {
                jm = m2;
                break;
              }
            }
          }
        }
      }

      if (jm == null) {
        throw new IllegalStateException("HandbookJobManager not configured in ServletContext");
      }

      HandbookJob job = jm.createAndSubmitJob(req.getInputStream());

      ObjectNode node = mapper.createObjectNode();
      node.put("jobId", job.jobId);
      node.put("status", "accepted");

      resp.setStatus(202);
      resp.setContentType("application/json");
      resp.getWriter().write(mapper.writeValueAsString(node));

    } catch (Exception e) {
      resp.setStatus(500);
      resp.setContentType("application/json");
      resp.getWriter()
          .write(
              """
                {"error":"Failed to accept upload: %s"}"""
                  .formatted(e.getMessage()));
    }
  }
}
