package com.gentoro.onemcp.management.jobs;

import com.gentoro.onemcp.OneMcp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;

/** Single-threaded asynchronous job manager with in-memory storage and cancellation support. */
public final class JobManager {
  private static final Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(JobManager.class);

  private final OneMcp oneMcp;
  private final JobStore store;
  private final ExecutorService executor;
  private final Map<JobType, JobHandler> handlers = new HashMap<>();

  public JobManager(OneMcp oneMcp, JobStore store) {
    this.oneMcp = oneMcp;
    this.store = store;
    // Per requirements: single-threaded executor
    this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "management-jobs"));
  }

  public void register(JobType type, JobHandler handler) {
    handlers.put(type, handler);
  }

  public String submit(
      JobType type, byte[] requestBytes, String contentType, Map<String, String> meta) {
    if (!handlers.containsKey(type)) {
      throw new IllegalArgumentException("No handler registered for type: " + type);
    }
    String id = UUID.randomUUID().toString();
    JobRecord record = new JobRecord(id, type, requestBytes, contentType, meta);
    store.put(record);

    record.future = executor.submit(() -> run(record));
    return id;
  }

  private void run(final JobRecord r) {
    if (r.cancelRequested) {
      // cancelled before start
      r.status = JobStatus.CANCELLED;
      r.finishedAt = Instant.now();
      return;
    }
    r.startedAt = Instant.now();
    r.status = JobStatus.RUNNING;

    JobHandler handler = handlers.get(r.type);
    try {
      JobContext ctx =
          new JobContext(
              oneMcp,
              r.id,
              r.type,
              () -> r.cancelRequested || Thread.currentThread().isInterrupted());
      JobExecution exec =
          handler.execute(
              ctx,
              r.requestBytes,
              r.requestContentType,
              new JobHandler.ProgressReporter() {
                @Override
                public void reportProgress(long total, long current, String message) {
                  if (total != -1 && current != -1) {
                    r.message = "[%d / %d] - %s".formatted(current, total, message);
                  }
                  r.message = message;
                }
              });
      if (r.cancelRequested || Thread.currentThread().isInterrupted()) {
        r.status = JobStatus.CANCELLED;
      } else {
        r.resultBytes = exec != null ? exec.resultBytes : null;
        r.resultContentType = exec != null ? exec.resultContentType : null;
        r.message = exec != null ? exec.message : null;
        r.status = JobStatus.DONE;
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      r.status = JobStatus.CANCELLED;
      r.message = "Cancelled";
    } catch (Exception e) {
      log.error("Job {} failed: {}", r.id, e.toString());
      r.status = JobStatus.FAILED;
      r.message = e.getMessage();
    } finally {
      r.finishedAt = Instant.now();
    }
  }

  public Optional<JobView> view(String id) {
    return store.get(id).map(JobView::fromRecord);
  }

  public Optional<ResultView> result(String id) {
    return store
        .get(id)
        .flatMap(
            r ->
                r.resultBytes != null
                    ? Optional.of(new ResultView(r.resultBytes, r.resultContentType))
                    : Optional.empty());
  }

  public boolean cancel(String id) {
    Optional<JobRecord> rec = store.get(id);
    if (rec.isEmpty()) return false;
    JobRecord r = rec.get();
    r.cancelRequested = true;
    if (r.future != null) {
      r.future.cancel(true);
    }
    // Mark as cancelled immediately to make state visible even if task never started
    if (r.status == JobStatus.PENDING || r.status == JobStatus.RUNNING) {
      r.status = JobStatus.CANCELLED;
      r.message = "Cancelled";
      r.finishedAt = java.time.Instant.now();
    }
    return true;
  }

  /** DTO for REST exposure. */
  public record JobView(
      String jobId,
      String type,
      String status,
      String message,
      Instant createdAt,
      Instant startedAt,
      Instant finishedAt) {
    static JobView fromRecord(JobRecord r) {
      return new JobView(
          r.id, r.type.name(), r.status.name(), r.message, r.createdAt, r.startedAt, r.finishedAt);
    }
  }

  /** Result holder for REST. */
  public record ResultView(byte[] bytes, String contentType) {}
}
