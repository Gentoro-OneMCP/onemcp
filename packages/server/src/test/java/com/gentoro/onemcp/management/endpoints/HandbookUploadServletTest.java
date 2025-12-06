package com.gentoro.onemcp.management.endpoints;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.gentoro.onemcp.management.jobs.JobManager;
import com.gentoro.onemcp.management.jobs.JobType;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.ServletTester;
import org.eclipse.jetty.http.HttpTester;
import org.junit.jupiter.api.*;

class HandbookUploadServletTest {

  @Test
  void acceptsUploadAndReturnsJobId() throws Exception {
    JobManager jobs = mock(JobManager.class);
    when(jobs.submit(eq(JobType.HANDBOOK_INGEST), any(byte[].class), anyString(), anyMap()))
        .thenReturn("123");

    ServletTester tester = new ServletTester();
    tester.setContextPath("/");

    // Register servlet instance with mocked JobManager
    ServletHolder holder = new ServletHolder(new HandbookUploadServlet(jobs));
    tester.getContext().addServlet(holder, "/handbook/upload");

    tester.start();

    HttpTester.Request req = HttpTester.newRequest();
    req.setMethod("PUT");
    req.setURI("/handbook/upload");
    req.setVersion("HTTP/1.1");
    req.setContent("abc");
    req.setHeader("Content-Type", "application/gzip");

    HttpTester.Response resp = HttpTester.parseResponse(tester.getResponses(req.generate()));

    assertEquals(202, resp.getStatus());
    assertTrue(resp.getContent().contains("\"jobId\":\"123\""));
  }
}
