package com.gentoro.onemcp.management;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.management.endpoints.*;
import com.gentoro.onemcp.management.jobs.InMemoryJobStore;
import com.gentoro.onemcp.management.jobs.JobManager;
import com.gentoro.onemcp.management.jobs.JobType;
import com.gentoro.onemcp.management.jobs.handlers.HandbookIngestHandler;
import com.gentoro.onemcp.management.jobs.handlers.RegressionRunHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;

/**
 * The ManagementServer class is responsible for managing HTTP endpoints related to handbook
 * ingestion, regression runs, and job management in a server environment. It registers a variety of
 * purpose-specific servlets to handle respective HTTP requests and integrates with the OneMcp
 * system and a JobManager for functionality.
 *
 * <p>Instances of this class handle job registrations and provide context-specific paths for the
 * servlets, while delegating execution and data storage to the underlying JobManager and associated
 * handlers.
 *
 * <p>Responsibilities: - Manage and register HTTP servlets for various operations. - Use the
 * JobManager to handle job types such as handbook ingestion and regression runs. - Provide
 * backward-compatible management endpoints for handbook operations.
 */
public final class ManagementServer {

  private final OneMcp oneMcp;
  private final JobManager jobs;

  public ManagementServer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;

    // New generalized in-memory job manager (single-thread)
    this.jobs = new JobManager(oneMcp, new InMemoryJobStore());
    this.jobs.register(JobType.HANDBOOK_INGEST, new HandbookIngestHandler());
    this.jobs.register(JobType.REGRESSION_RUN, new RegressionRunHandler(oneMcp));
  }

  private String contextPath() {
    return "/mng";
  }

  /** Register all management-related servlets with the Jetty context handler. */
  public void register() {
    var ctx = oneMcp.httpServer().getContextHandler();

    // Upload uses new generalized job manager under the hood (submits HANDBOOK_INGEST)
    ctx.addServlet(
        new ServletHolder(new HandbookUploadServlet(jobs)),
        "%s/handbook/upload".formatted(contextPath()));

    ctx.addServlet(
        new ServletHolder(new HandbookDownloadServlet(oneMcp)),
        "%s/handbook/download".formatted(contextPath()));

    // Status endpoint (backward compatible JSON shape with "details")
    ctx.addServlet(
        new ServletHolder(new HandbookStatusCompatServlet(jobs)),
        "%s/handbook/status/*".formatted(contextPath()));

    ctx.addServlet(
        new ServletHolder(new HandbookHashesServlet(oneMcp)),
        "%s/handbook/hashes".formatted(contextPath()));

    // Generic jobs status and cancel
    ctx.addServlet(
        new ServletHolder(new JobsStatusServlet(jobs)),
        "%s/jobs/status/*".formatted(contextPath()));
    ctx.addServlet(
        new ServletHolder(new JobsCancelServlet(jobs)), "%s/jobs/*".formatted(contextPath()));

    // Regression endpoints
    ctx.addServlet(
        new ServletHolder(new RegressionRunServlet(jobs)),
        "%s/handbook/regression/run".formatted(contextPath()));
    ctx.addServlet(
        new ServletHolder(new JobsStatusServlet(jobs)),
        "%s/handbook/regression/status/*".formatted(contextPath()));
    ctx.addServlet(
        new ServletHolder(new RegressionSummaryServlet(jobs)),
        "%s/handbook/regression/summary".formatted(contextPath()));
  }
}
