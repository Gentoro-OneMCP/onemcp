package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.management.jobs.JobManager;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/** GET /mng/jobs/status/{id} — returns job status for any job type. */
public final class JobsStatusServlet extends HttpServlet {
  private final JobManager jobs;
  private final ObjectMapper mapper = new ObjectMapper();

  public JobsStatusServlet(JobManager jobs) {
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

    ObjectNode node = mapper.createObjectNode();
    var v = view.get();
    node.put("jobId", v.jobId());
    node.put("type", v.type());
    node.put("status", v.status());
    if (v.message() != null) node.put("message", v.message());
    if (v.createdAt() != null) node.put("createdAt", v.createdAt().toString());
    if (v.startedAt() != null) node.put("startedAt", v.startedAt().toString());
    if (v.finishedAt() != null) node.put("finishedAt", v.finishedAt().toString());

    resp.setStatus(200);
    resp.setContentType("application/json");
    resp.getWriter().write(mapper.writeValueAsString(node));
  }
}
