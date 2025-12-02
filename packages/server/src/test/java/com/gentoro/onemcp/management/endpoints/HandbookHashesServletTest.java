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

class HandbookHashesServletTest {

  @Test
  void returnsHashMapJson() throws Exception {
    Path hb = Files.createTempDirectory("hb");
    Files.writeString(hb.resolve("x.txt"), "hello");

    Handbook handbook = mock(Handbook.class);
    when(handbook.location()).thenReturn(hb);

    OneMcp mcp = mock(OneMcp.class);
    when(mcp.handbook()).thenReturn(handbook);

    ServletTester tester = new ServletTester();
    tester.addServlet(new ServletHolder(new HandbookHashesServlet(mcp)), "/handbook/hashes");
    tester.start();

    HttpTester.Request req = HttpTester.newRequest();
    req.setMethod("GET");
    req.setURI("/handbook/hashes");
    req.setVersion("HTTP/1.1");

    HttpTester.Response resp = HttpTester.parseResponse(tester.getResponses(req.generate()));

    assertEquals(200, resp.getStatus());
    assertTrue(resp.getContent().contains("x.txt"));
  }
}
