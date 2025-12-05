package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.management.async.*;
import jakarta.servlet.http.*;
import java.io.IOException;

/**
 * GET /mng/handbook/status/{jobId}
 *
 * <p>Returns status for an asynchronous handbook upload job.
 */
public final class HandbookStatusServlet extends HttpServlet {

  private final HandbookJobManager jobs;
  private final ObjectMapper mapper = new ObjectMapper();

  public HandbookStatusServlet(HandbookJobManager jobs) {
    this.jobs = jobs;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String jobId = req.getPathInfo();
    if (jobId == null || jobId.length() <= 1) {
      resp.sendError(400, "Missing jobId");
      return;
    }
    jobId = jobId.substring(1);

    HandbookJob job = jobs.getJob(jobId);
    if (job == null) {
      resp.sendError(404, "Unknown jobId");
      return;
    }

    ObjectNode node = mapper.createObjectNode();
    node.put("jobId", job.jobId);
    node.put("status", job.status.toString());
    if (job.details != null) node.put("details", job.details);

    resp.setStatus(200);
    resp.setContentType("application/json");
    resp.getWriter().write(mapper.writeValueAsString(node));
  }
}
