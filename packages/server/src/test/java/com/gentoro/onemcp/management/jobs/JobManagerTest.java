package com.gentoro.onemcp.management.jobs;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Agent;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.handbook.model.regression.RegressionSuite;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JobManagerTest {

  static class DummyHandbook implements Handbook {
    private final Path loc;

    DummyHandbook(Path loc) {
      this.loc = loc;
    }

    @Override
    public Path location() {
      return loc;
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
      return Map.of();
    }

    @Override
    public Optional<RegressionSuite> optionalRegressionSuite(String relativePath) {
      return Optional.empty();
    }

    @Override
    public RegressionSuite regressionSuite(String relativePath) {
      throw new UnsupportedOperationException();
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
      return "dummy";
    }
  }

  static class DummyMcp extends OneMcp {
    private final Handbook hb;

    DummyMcp(Handbook hb) {
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

  @Test
  @DisplayName("JobManager executes a simple handler and stores result")
  void executesHandlerAndStoresResult() throws Exception {
    var mcp = new DummyMcp(new DummyHandbook(Path.of(".")));
    var jm = new JobManager(mcp, new InMemoryJobStore());
    jm.register(
        JobType.HANDBOOK_INGEST,
        (ctx, req, ct, pr) -> new JobExecution("ok".getBytes(), "text/plain", "done"));

    String id = jm.submit(JobType.HANDBOOK_INGEST, new byte[] {}, "text/plain", Map.of());

    // await completion
    JobManager.JobView view = awaitStatus(jm, id, JobStatus.DONE, Duration.ofSeconds(3));
    assertEquals(JobStatus.DONE.name(), view.status());

    var result = jm.result(id);
    assertTrue(result.isPresent());
    assertEquals("text/plain", result.get().contentType());
    assertEquals("ok", new String(result.get().bytes()));
  }

  @Test
  @DisplayName("JobManager cancels a long-running job")
  void cancelsJob() throws Exception {
    var mcp = new DummyMcp(new DummyHandbook(Path.of(".")));
    var jm = new JobManager(mcp, new InMemoryJobStore());

    jm.register(
        JobType.REGRESSION_RUN,
        (ctx, req, ct, pr) -> {
          // Simulate work and cooperate with cancellation
          for (int i = 0; i < 50; i++) {
            if (ctx.isCancelled()) throw new InterruptedException("cancelled");
            TimeUnit.MILLISECONDS.sleep(20);
          }
          return new JobExecution(null, null, "finished");
        });

    String id = jm.submit(JobType.REGRESSION_RUN, new byte[] {}, "application/json", Map.of());
    // Immediately request cancel
    boolean accepted = jm.cancel(id);
    assertTrue(accepted);

    JobManager.JobView view =
        awaitAnyStatus(
            jm,
            id,
            new JobStatus[] {JobStatus.CANCELLED, JobStatus.DONE, JobStatus.FAILED},
            Duration.ofSeconds(3));
    assertEquals(JobStatus.CANCELLED.name(), view.status());
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

  private static JobManager.JobView awaitAnyStatus(
      JobManager jm, String id, JobStatus[] desired, Duration timeout) throws Exception {
    long end = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < end) {
      var v = jm.view(id);
      if (v.isPresent()) {
        for (JobStatus d : desired) if (d.name().equals(v.get().status())) return v.get();
      }
      Thread.sleep(20);
    }
    fail("Timeout waiting for status in set");
    return null; // Unreachable
  }
}
