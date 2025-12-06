package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.management.jobs.JobManager;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * GET /mng/handbook/status/{jobId}
 *
 * <p>Backward-compatible status response for handbook upload jobs, using field name "details" to
 * match the legacy shape.
 */
public final class HandbookStatusCompatServlet extends HttpServlet {
  private final JobManager jobs;
  private final ObjectMapper mapper = new ObjectMapper();

  public HandbookStatusCompatServlet(JobManager jobs) {
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

    var view = jobs.view(jobId);
    if (view.isEmpty()) {
      resp.sendError(404, "Unknown jobId");
      return;
    }

    var v = view.get();
    ObjectNode node = mapper.createObjectNode();
    node.put("jobId", v.jobId());
    node.put("status", v.status());
    if (v.message() != null) node.put("details", v.message());

    resp.setStatus(200);
    resp.setContentType("application/json");
    resp.getWriter().write(mapper.writeValueAsString(node));
  }
}
