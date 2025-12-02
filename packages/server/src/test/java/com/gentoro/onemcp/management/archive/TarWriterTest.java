package com.gentoro.onemcp.management.archive;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.*;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class TarWriterTest {

  @TempDir Path temp;

  @Test
  void writesTarGzipOfDirectory() throws Exception {
    // Arrange
    Path dir = temp.resolve("src");
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("a.txt"), "Hello A");
    Files.writeString(dir.resolve("b.txt"), "Hello B");

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // Act
    TarWriter.writeDirectoryAsTarGzip(dir, out);

    // Assert: must be valid gzip
    assertDoesNotThrow(() -> new GZIPInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertTrue(out.toByteArray().length > 100, "Archive should contain entries");
  }

  @Test
  void handlesDeepDirectories() throws Exception {
    Path deep = temp.resolve("root/a/b/c/d/e");
    Files.createDirectories(deep);
    Files.writeString(deep.resolve("file.txt"), "OK");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TarWriter.writeDirectoryAsTarGzip(temp.resolve("root"), out);

    assertTrue(out.toByteArray().length > 0);
  }

  @Test
  void failsOnTooLongName() throws Exception {
    Path dir = temp.resolve("long");
    Files.createDirectories(dir);

    // Create >100 byte filename
    String name = "a".repeat(150);
    Files.writeString(dir.resolve(name), "x");

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThrows(IOException.class, () -> TarWriter.writeDirectoryAsTarGzip(dir, out));
  }
}
