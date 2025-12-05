package com.gentoro.onemcp.management.async;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.management.archive.TarWriter;
import java.io.*;
import java.nio.file.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class HandbookJobManagerTest {

  @TempDir Path temp;

  OneMcp oneMcp;
  HandbookJobManager manager;

  Path handbookDir;

  @BeforeEach
  void setup() {
    oneMcp = mock(OneMcp.class);
    manager = new HandbookJobManager(oneMcp);

    handbookDir = temp.resolve("handbook");
    Handbook hb = mock(Handbook.class);
    when(hb.location()).thenReturn(handbookDir);

    when(oneMcp.handbook()).thenReturn(hb);
  }

  /** Creates a tar.gz of the given directory */
  private byte[] archive(Path dir) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TarWriter.writeDirectoryAsTarGzip(dir, out);
    return out.toByteArray();
  }

  @Test
  void asyncJobProcessesSuccessfully() throws Exception {
    Path source = temp.resolve("src");
    Files.createDirectories(source);
    Files.writeString(source.resolve("a.txt"), "AAA");

    HandbookJob job = manager.createAndSubmitJob(new ByteArrayInputStream(archive(source)));

    // Wait for job to complete
    for (int i = 0; i < 50; i++) {
      if (job.status == HandbookJobStatus.DONE) break;
      Thread.sleep(50);
    }

    assertEquals(HandbookJobStatus.DONE, job.status);
    assertTrue(Files.exists(handbookDir.resolve("a.txt")));

    verify(oneMcp, atLeastOnce()).reloadHandbook();
  }

  @Test
  void jobFailsOnCorruptArchive() throws Exception {
    byte[] bad = new byte[] {1, 2, 3, 4};

    HandbookJob job = manager.createAndSubmitJob(new ByteArrayInputStream(bad));

    for (int i = 0; i < 50; i++) {
      if (job.status == HandbookJobStatus.FAILED) break;
      Thread.sleep(50);
    }

    assertEquals(HandbookJobStatus.FAILED, job.status);
    assertNotNull(job.details);
  }
}
