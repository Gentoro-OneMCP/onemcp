package com.gentoro.onemcp.management.archive;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class IoUtilTest {

  @TempDir Path temp;

  @Test
  void copiesStreamToFile() throws Exception {
    Path dest = temp.resolve("file.txt");
    IoUtil.copyStream(new ByteArrayInputStream("hello".getBytes()), dest);

    assertTrue(Files.exists(dest));
    assertEquals("hello", Files.readString(dest));
  }

  @Test
  void preventsZipSlip() {
    Path base = temp.resolve("safe");
    Path result = IoUtil.secureResolve(base, "good.txt");
    assertTrue(result.startsWith(base));
  }

  @Test
  void detectsZipSlip() {
    Path base = temp.resolve("safe");
    assertThrows(Exception.class, () -> IoUtil.secureResolve(base, "../../evil.txt"));
  }

  @Test
  void silentDeleteDirDeletesEverything() throws Exception {
    Path d = temp.resolve("del");
    Files.createDirectories(d);
    Files.writeString(d.resolve("x.txt"), "x");

    IoUtil.silentDeleteDir(d);
    assertFalse(Files.exists(d));
  }
}
