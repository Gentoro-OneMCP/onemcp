package com.gentoro.onemcp.management.endpoints;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import java.nio.file.*;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.ServletTester;
import org.eclipse.jetty.http.HttpTester;
import org.junit.jupiter.api.*;

class HandbookDownloadServletTest {

  @Test
  void returnsTarGzipStream() throws Exception {
    Path handbook = Files.createTempDirectory("hb");
    Files.writeString(handbook.resolve("a.txt"), "Hello");

    Handbook hb = mock(Handbook.class);
    when(hb.location()).thenReturn(handbook);

    OneMcp mcp = mock(OneMcp.class);
    when(mcp.handbook()).thenReturn(hb);

    ServletTester tester = new ServletTester();
    tester.addServlet(new ServletHolder(new HandbookDownloadServlet(mcp)), "/handbook");
    tester.start();

    HttpTester.Request req = HttpTester.newRequest();
    req.setMethod("GET");
    req.setURI("/handbook");
    req.setVersion("HTTP/1.1");

    HttpTester.Response resp = HttpTester.parseResponse(tester.getResponses(req.generate()));

    assertEquals(200, resp.getStatus());
    assertEquals("application/gzip", resp.get("Content-Type"));
    assertTrue(resp.getContentBytes().length > 100);
  }
}
