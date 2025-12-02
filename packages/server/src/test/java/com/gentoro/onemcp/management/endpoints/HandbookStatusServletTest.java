package com.gentoro.onemcp.management.endpoints;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.management.async.*;
import java.nio.file.*;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.ServletTester;
import org.eclipse.jetty.http.HttpTester;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

class HandbookStatusServletTest {

  @Test
  void returnsStatusForKnownJob() throws Exception {
    HandbookJobManager manager = Mockito.mock(HandbookJobManager.class);

    HandbookJob job = new HandbookJob("abc", Paths.get("/tmp"));
    job.status = HandbookJobStatus.DONE;

    Mockito.when(manager.getJob(Mockito.anyString())).thenReturn(job);

    ServletTester tester = new ServletTester();
    tester.addServlet(new ServletHolder(new HandbookStatusServlet(manager)), "/handbook/status/*");

    tester.start();

    HttpTester.Request req = HttpTester.newRequest();
    req.setMethod("GET");
    req.setURI("/handbook/status/abc");
    req.setVersion("HTTP/1.1");

    HttpTester.Response resp = HttpTester.parseResponse(tester.getResponses(req.generate()));

    assertEquals(200, resp.getStatus());
    assertTrue(resp.getContent().contains("DONE"));
  }
}
