package com.gentoro.onemcp.management.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.management.archive.HashUtil;
import jakarta.servlet.http.*;
import java.io.IOException;

/**
 * GET /mng/handbook/hashes
 *
 * <p>Returns a JSON map of relative file paths to SHA-256 hashes.
 */
public final class HandbookHashesServlet extends HttpServlet {

  private final OneMcp oneMcp;
  private final ObjectMapper mapper = new ObjectMapper();

  public HandbookHashesServlet(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    var root = oneMcp.handbook().location();
    var hashes = HashUtil.computeDirectoryHashes(root);

    resp.setStatus(200);
    resp.setContentType("application/json");
    resp.getWriter().write(mapper.writeValueAsString(hashes));
  }
}
