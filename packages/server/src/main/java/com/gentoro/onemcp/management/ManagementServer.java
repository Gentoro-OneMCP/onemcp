package com.gentoro.onemcp.management;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.management.async.HandbookJobManager;
import com.gentoro.onemcp.management.endpoints.*;
import org.eclipse.jetty.ee10.servlet.ServletHolder;

/**
 * Registers all management HTTP endpoints under {@code /mng}.
 *
 * <p>This class acts only as endpoint registration glue. All servlet logic and background
 * processing is implemented in separate, testable components.
 *
 * <p>Endpoints:
 *
 * <ul>
 *   <li>PUT /mng/handbook — async upload
 *   <li>GET /mng/handbook — download archive
 *   <li>GET /mng/handbook/status/{id} — async job polling
 *   <li>GET /mng/handbook/hashes — SHA-256 hash map
 * </ul>
 */
public final class ManagementServer {

  private final OneMcp oneMcp;
  private final HandbookJobManager jobManager;

  public ManagementServer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    this.jobManager = new HandbookJobManager(oneMcp);
  }

  private String contextPath() {
    return "/mng";
  }

  /** Register all management-related servlets with the Jetty context handler. */
  public void register() {
    var ctx = oneMcp.httpServer().getContextHandler();

    ctx.addServlet(
        new ServletHolder(new HandbookUploadServlet(jobManager)),
        "%s/handbook/upload".formatted(contextPath()));

    ctx.addServlet(
        new ServletHolder(new HandbookDownloadServlet(oneMcp)),
        "%s/handbook/download".formatted(contextPath()));

    ctx.addServlet(
        new ServletHolder(new HandbookStatusServlet(jobManager)),
        "%s/handbook/status/*".formatted(contextPath()));

    ctx.addServlet(
        new ServletHolder(new HandbookHashesServlet(oneMcp)),
        "%s/handbook/hashes".formatted(contextPath()));
  }
}
