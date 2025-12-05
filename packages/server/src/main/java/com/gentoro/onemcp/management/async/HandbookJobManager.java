package com.gentoro.onemcp.management.async;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.management.archive.IoUtil;
import com.gentoro.onemcp.management.archive.TarReader;
import com.gentoro.onemcp.utility.FileUtility;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import org.slf4j.Logger;

/**
 * Central service responsible for:
 *
 * <ul>
 *   <li>Creating new handbook upload jobs
 *   <li>Executing uploads asynchronously
 *   <li>Replacing handbook contents
 *   <li>Reloading the in-memory handbook
 * </ul>
 */
public final class HandbookJobManager {

  private static final Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(HandbookJobManager.class);

  private final OneMcp oneMcp;

  private final ExecutorService executor =
      Executors.newSingleThreadExecutor(r -> new Thread(r, "handbook-uploader"));

  private final Map<String, HandbookJob> jobs = new ConcurrentHashMap<>();

  public HandbookJobManager(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /** Create a new job and start asynchronous processing. */
  public HandbookJob createAndSubmitJob(InputStream uploadStream) throws Exception {
    Path tempDir = Files.createTempDirectory("handbook-job-");
    Path archive = tempDir.resolve("upload.tar.gz");
    IoUtil.copyStream(uploadStream, archive);

    String jobId = UUID.randomUUID().toString();
    HandbookJob job = new HandbookJob(jobId, tempDir);
    jobs.put(jobId, job);

    executor.submit(() -> processJob(job, archive));

    return job;
  }

  public Map<String, HandbookJob> getJobs() {
    return Collections.unmodifiableMap(jobs);
  }

  /** Retrieve a job by ID, or {@code null} if unknown. */
  public HandbookJob getJob(String id) {
    return jobs.get(id);
  }

  // --------------------------------------------------------------------
  // Background processing logic
  // --------------------------------------------------------------------

  private void processJob(HandbookJob job, Path archive) {
    job.status = HandbookJobStatus.RUNNING;
    Path targetRoot = oneMcp.handbook().location();

    try {
      // 1. Extract to staging directory
      Path extracted = job.tempDirectory.resolve("extracted");
      Files.createDirectories(extracted);

      job.details = "Extracting archive to staging directory";
      try (var in = Files.newInputStream(archive)) {
        TarReader.extractTarGzipSecure(in, extracted);
      }

      // 2. Replace handbook directory atomically-ish
      if (Files.exists(targetRoot)) FileUtility.deleteDir(targetRoot, true);
      Files.createDirectories(targetRoot);
      FileUtility.copyDirectory(extracted, targetRoot);

      // 3. Reload
      oneMcp.reloadHandbook();

      job.status = HandbookJobStatus.DONE;
    } catch (Exception e) {
      log.error("Handbook upload failed", e);
      job.status = HandbookJobStatus.FAILED;
      job.details = ExceptionUtil.formatCompactStackTrace(e);
    } finally {
      IoUtil.silentDeleteDir(job.tempDirectory);
    }
  }
}
