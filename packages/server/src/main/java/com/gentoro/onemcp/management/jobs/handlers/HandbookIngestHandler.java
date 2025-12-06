package com.gentoro.onemcp.management.jobs.handlers;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.management.archive.IoUtil;
import com.gentoro.onemcp.management.archive.TarReader;
import com.gentoro.onemcp.management.jobs.JobContext;
import com.gentoro.onemcp.management.jobs.JobExecution;
import com.gentoro.onemcp.management.jobs.JobHandler;
import com.gentoro.onemcp.utility.FileUtility;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;

/**
 * Executes a handbook ingest by replacing the handbook directory with contents from the tar.gz
 * uploaded archive, then triggering a reload.
 */
public final class HandbookIngestHandler implements JobHandler {
  private static final Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(HandbookIngestHandler.class);

  @Override
  public JobExecution execute(
      JobContext ctx,
      byte[] requestBytes,
      String requestContentType,
      ProgressReporter progressReporter)
      throws Exception {
    OneMcp oneMcp = ctx.oneMcp();
    Path tempDir = Files.createTempDirectory("handbook-ingest-");
    try {
      if (ctx.isCancelled()) throw new InterruptedException("Cancelled before start");

      progressReporter.reportProgress(-1, -1, "Extracting handbook archive");
      Path extracted = tempDir.resolve("extracted");
      Files.createDirectories(extracted);

      // Extract archive from request bytes
      try (var in = new ByteArrayInputStream(requestBytes)) {
        TarReader.extractTarGzipSecure(in, extracted);
      }

      if (ctx.isCancelled()) throw new InterruptedException("Cancelled after extract");

      // Replace handbook directory
      Path targetRoot = oneMcp.handbook().location();
      if (Files.exists(targetRoot)) FileUtility.deleteDir(targetRoot, true);
      Files.createDirectories(targetRoot);
      FileUtility.copyDirectory(extracted, targetRoot);

      if (ctx.isCancelled()) throw new InterruptedException("Cancelled after replace");

      progressReporter.reportProgress(-1, -1, "Processing handbook");
      oneMcp.reloadHandbook(progressReporter);

      return new JobExecution(null, null, "Handbook ingest completed");
    } finally {
      IoUtil.silentDeleteDir(tempDir);
    }
  }
}
