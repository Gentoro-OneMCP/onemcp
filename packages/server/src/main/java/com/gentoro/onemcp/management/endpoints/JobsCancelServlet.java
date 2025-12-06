package com.gentoro.onemcp.management.endpoints;

import com.gentoro.onemcp.management.jobs.JobManager;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/** DELETE /mng/jobs/{id} — cancels a job if possible. */
public final class JobsCancelServlet extends HttpServlet {
  private final JobManager jobs;

  public JobsCancelServlet(JobManager jobs) {
    this.jobs = jobs;
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String jobId = req.getPathInfo();
    if (jobId == null || jobId.length() <= 1) {
      resp.sendError(400, "Missing jobId");
      return;
    }
    jobId = jobId.substring(1);

    boolean ok = jobs.cancel(jobId);
    if (!ok) {
      resp.sendError(404, "Unknown jobId");
      return;
    }
    resp.setStatus(202);
  }
}
