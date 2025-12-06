package com.gentoro.onemcp.management.jobs;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Agent;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.handbook.model.regression.RegressionSuite;
import com.gentoro.onemcp.handbook.model.regression.TestCase;
import com.gentoro.onemcp.management.jobs.handlers.RegressionRunHandler;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RegressionRunHandlerTest {

  static class HB implements Handbook {
    private final Map<String, RegressionSuite> suites;

    HB(Map<String, RegressionSuite> suites) {
      this.suites = suites;
    }

    @Override
    public Path location() {
      return Path.of(".");
    }

    @Override
    public Agent agent() {
      return null;
    }

    @Override
    public Optional<Api> optionalApi(String slug) {
      return Optional.empty();
    }

    @Override
    public Api api(String slug) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Api> apis() {
      return Map.of();
    }

    @Override
    public Map<String, RegressionSuite> regressionSuites() {
      return suites;
    }

    @Override
    public Optional<RegressionSuite> optionalRegressionSuite(String relativePath) {
      return Optional.ofNullable(suites.get(relativePath));
    }

    @Override
    public RegressionSuite regressionSuite(String relativePath) {
      return suites.get(relativePath);
    }

    @Override
    public Map<String, String> documentation() {
      return Map.of();
    }

    @Override
    public Optional<String> optionalDocumentation(String relativePath) {
      return Optional.empty();
    }

    @Override
    public String documentation(String relativePath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OneMcp oneMcp() {
      return null;
    }

    @Override
    public String name() {
      return "hb";
    }
  }

  static class MCP extends OneMcp {
    private final Handbook hb;

    MCP(Handbook hb) {
      super(new String[] {});
      this.hb = hb;
    }

    @Override
    public Configuration configuration() {
      return new YAMLConfiguration();
    }

    @Override
    public Handbook handbook() {
      return hb;
    }
  }

  private static RegressionSuite suite(String... testNames) {
    RegressionSuite s = new RegressionSuite();
    List<TestCase> list = new ArrayList<>();
    for (String n : testNames) {
      TestCase tc = new TestCase();
      tc.setDisplayName(n);
      tc.setPrompt("prompt: " + n);
      tc.setAssertion("assert: " + n);
      list.add(tc);
    }
    s.setTests(list);
    return s;
  }

  @Test
  @DisplayName("RegressionRunHandler produces JSON summary with counts")
  void regressionSummary() throws Exception {
    Map<String, RegressionSuite> suites = new HashMap<>();
    suites.put("foo.yaml", suite("a", "b", "c"));
    suites.put("bar.yaml", suite("x"));
    var hb = new HB(suites);
    var mcp = new MCP(hb);

    var jm = new JobManager(mcp, new InMemoryJobStore());
    jm.register(JobType.REGRESSION_RUN, new RegressionRunHandler(mcp));

    // RegressionRunHandler expects a RegressionRequest JSON with a "paths" field
    byte[] body =
        new ObjectMapper().writeValueAsBytes(Map.of("paths", List.of("foo.yaml", "bar.yaml")));
    String id = jm.submit(JobType.REGRESSION_RUN, body, "application/json", Map.of());

    JobManager.JobView view = awaitStatus(jm, id, JobStatus.DONE, Duration.ofSeconds(3));
    assertEquals(JobStatus.DONE.name(), view.status());

    var result = jm.result(id);
    assertTrue(result.isPresent());
    assertEquals("application/json", result.get().contentType());

    JsonNode summary = new ObjectMapper().readTree(result.get().bytes());
    int passed = summary.path("passed").asInt();
    int failed = summary.path("failed").asInt();
    assertEquals(4, passed + failed);
    assertTrue(summary.path("durationMs").asLong() >= 1);
    assertEquals(4, summary.path("testCases").size());
  }

  private static JobManager.JobView awaitStatus(
      JobManager jm, String id, JobStatus desired, Duration timeout) throws Exception {
    long end = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < end) {
      var v = jm.view(id);
      if (v.isPresent() && desired.name().equals(v.get().status())) return v.get();
      Thread.sleep(20);
    }
    fail("Timeout waiting for status " + desired);
    return null; // Unreachable
  }
}
