package com.gentoro.onemcp.management.endpoints;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.management.async.HandbookJob;
import com.gentoro.onemcp.management.async.HandbookJobManager;
import java.nio.file.*;
import org.eclipse.jetty.ee10.servlet.ServletTester;
import org.eclipse.jetty.http.HttpTester;
import org.junit.jupiter.api.*;

class HandbookUploadServletTest {

  @Test
  void acceptsUploadAndReturnsJobId() throws Exception {
    OneMcp mcp = mock(OneMcp.class);
    HandbookJobManager manager = mock(HandbookJobManager.class);

    var job = new HandbookJob("123", Paths.get("/tmp"));
    when(manager.createAndSubmitJob(any())).thenReturn(job);

    ServletTester tester = new ServletTester();
    tester
        .addServlet(HandbookUploadServlet.class, "/handbook")
        .setInitParameter("jobManager", "ignored");
    tester.setAttribute("jobManager", manager);

    tester.start();

    HttpTester.Request req = HttpTester.newRequest();
    req.setMethod("PUT");
    req.setURI("/handbook");
    req.setVersion("HTTP/1.1");
    req.setContent("abc");

    HttpTester.Response resp = HttpTester.parseResponse(tester.getResponses(req.generate()));

    assertEquals(202, resp.getStatus());
    assertTrue(resp.getContent().contains("123"));
  }
}
