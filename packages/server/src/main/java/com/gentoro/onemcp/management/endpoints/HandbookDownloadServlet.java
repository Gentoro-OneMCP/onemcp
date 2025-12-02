package com.gentoro.onemcp.management.endpoints;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.management.archive.TarWriter;
import jakarta.servlet.http.*;
import java.io.IOException;

/**
 * GET /mng/handbook
 *
 * <p>Returns a gzip-compressed tar archive of the current handbook content.
 */
public final class HandbookDownloadServlet extends HttpServlet {

  private final OneMcp oneMcp;

  public HandbookDownloadServlet(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    var root = oneMcp.handbook().location();

    resp.setStatus(200);
    resp.setContentType("application/gzip");
    resp.setHeader("Content-Disposition", "attachment; filename=handbook.tar.gz");

    TarWriter.writeDirectoryAsTarGzip(root, resp.getOutputStream());
  }
}
