package com.gentoro.onemcp.management.archive;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.*;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class TarReaderTest {

  @TempDir Path temp;

  /** Creates a minimal tar.gz containing one file. */
  private byte[] createSmallTarGz() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzos = new GZIPOutputStream(out)) {
      // Write tar header for file "f.txt"
      byte[] header = new byte[512];
      byte[] name = "f.txt".getBytes();
      System.arraycopy(name, 0, header, 0, name.length);

      // size = 5 bytes
      String sizeOctal = String.format("%011o", 5);
      System.arraycopy(sizeOctal.getBytes(), 0, header, 124, sizeOctal.length());
      header[156] = '0';

      // compute checksum
      for (int i = 148; i < 156; i++) header[i] = ' ';
      long sum = 0;
      for (byte b : header) sum += (b & 0xFF);
      String chk = String.format("%06o", sum);
      System.arraycopy(chk.getBytes(), 0, header, 148, chk.length());
      header[154] = 0;
      header[155] = ' ';

      gzos.write(header);
      gzos.write("Hello".getBytes());

      // padding
      gzos.write(new byte[512 - 5]);

      gzos.write(new byte[512]); // end block
      gzos.write(new byte[512]);
    }
    return out.toByteArray();
  }

  @Test
  void extractsFileCorrectly() throws Exception {
    Path out = temp.resolve("extract");
    Files.createDirectories(out);

    byte[] tar = createSmallTarGz();

    TarReader.extractTarGzipSecure(new ByteArrayInputStream(tar), out);

    Path file = out.resolve("f.txt");
    assertTrue(Files.exists(file));
    assertEquals("Hello", Files.readString(file));
  }

  @Test
  void blocksZipSlip() throws Exception {
    // Create exploit tar with ../../evil.txt
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (GZIPOutputStream gzos = new GZIPOutputStream(buffer)) {
      byte[] header = new byte[512];
      byte[] name = "../../evil.txt".getBytes();
      System.arraycopy(name, 0, header, 0, name.length);

      // minimal checksum hack
      for (int i = 148; i < 156; i++) header[i] = ' ';
      gzos.write(header);
      gzos.write(new byte[512]);
      gzos.write(new byte[512]);
    }

    assertThrows(
        Exception.class,
        () -> TarReader.extractTarGzipSecure(new ByteArrayInputStream(buffer.toByteArray()), temp));
  }
}
