package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.management.jobs.JobManager;
import com.gentoro.onemcp.management.jobs.JobType;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

/** POST /mng/handbook/regression/run — accepts a JSON body and returns a job id. */
public final class RegressionRunServlet extends HttpServlet {
  private final JobManager jobs;
  private final ObjectMapper mapper = new ObjectMapper();

  public RegressionRunServlet(JobManager jobs) {
    this.jobs = jobs;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
      byte[] body = req.getInputStream().readAllBytes();
      if (body.length == 0) {
        resp.sendError(400, "Empty request body");
        return;
      }
      String jobId =
          jobs.submit(JobType.REGRESSION_RUN, body, "application/json", Collections.emptyMap());
      ObjectNode node = mapper.createObjectNode();
      node.put("jobId", jobId);
      node.put("status", "accepted");
      resp.setStatus(202);
      resp.setContentType("application/json");
      resp.getWriter().write(mapper.writeValueAsString(node));
    } catch (Exception e) {
      resp.sendError(500, "Failed to submit regression run: " + e.getMessage());
    }
  }
}
