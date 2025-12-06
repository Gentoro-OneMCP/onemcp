package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.management.jobs.JobManager;
import com.gentoro.onemcp.management.jobs.JobStatus;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/** GET /mng/handbook/regression/summary?id=JOB_ID — returns JSON summary when available. */
public final class RegressionSummaryServlet extends HttpServlet {
  private final JobManager jobs;
  private final ObjectMapper mapper = new ObjectMapper();

  public RegressionSummaryServlet(JobManager jobs) {
    this.jobs = jobs;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String jobId = req.getParameter("id");
    if (jobId == null || jobId.isBlank()) {
      resp.sendError(400, "Missing required query parameter 'id'");
      return;
    }

    var view = jobs.view(jobId);
    if (view.isEmpty()) {
      resp.sendError(404, "Unknown jobId");
      return;
    }

    String status = view.get().status();
    if (JobStatus.PENDING.name().equals(status) || JobStatus.RUNNING.name().equals(status)) {
      // still processing
      ObjectNode node = mapper.createObjectNode();
      node.put("jobId", view.get().jobId());
      node.put("status", status);
      if (view.get().message() != null) node.put("message", view.get().message());
      resp.setStatus(202);
      resp.setContentType("application/json");
      resp.getWriter().write(mapper.writeValueAsString(node));
      return;
    }

    if (JobStatus.FAILED.name().equals(status) || JobStatus.CANCELLED.name().equals(status)) {
      resp.sendError(
          409,
          "Job status: "
              + status
              + (view.get().message() != null ? (" — " + view.get().message()) : ""));
      return;
    }

    var result = jobs.result(jobId);
    if (result.isEmpty()) {
      resp.sendError(404, "No summary available for jobId");
      return;
    }
    var r = result.get();
    resp.setStatus(200);
    resp.setContentType(r.contentType());
    resp.getOutputStream().write(r.bytes());
  }
}
