package com.gentoro.onemcp.management.archive;

import java.io.*;
import java.nio.file.*;
import java.util.zip.GZIPInputStream;

/**
 * Secure tar.gz extractor with ZipSlip prevention.
 *
 * <p>Only supports regular files and directories (typeflag '0' and '5').
 */
public final class TarReader {

  private TarReader() {}

  /** Extract a .tar.gz stream to a directory, validating paths. */
  public static void extractTarGzipSecure(InputStream gzipInput, Path targetDir)
      throws IOException {
    try (GZIPInputStream gzis = new GZIPInputStream(gzipInput)) {
      extractTarSecure(gzis, targetDir);
    }
  }

  // --------------------------------------------------------------------

  public static void extractTarSecure(InputStream in, Path targetDir) throws IOException {
    byte[] header = new byte[512];

    while (true) {
      int read = readFully(in, header, 0, 512);
      if (read == -1 || isAllZeros(header)) {
        break; // end-of-archive
      }

      String name = getString(header, 0, 100);
      if (name.isEmpty()) continue;

      byte type = header[156];
      long size = parseOctal(header, 124, 12);

      Path output = IoUtil.secureResolve(targetDir, name);

      if (type == '5') {
        Files.createDirectories(output);
        continue;
      }

      if (type == '0' || type == 0) {
        if (output.getParent() != null) Files.createDirectories(output.getParent());

        try (OutputStream out =
            Files.newOutputStream(
                output, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
          copyFixedSize(in, out, size);
        }

        long pad = size % 512;
        if (pad != 0) in.skipNBytes(512 - pad);
        continue;
      }

      // Skip unknown record types
      if (size > 0) in.skipNBytes(size + ((size % 512 != 0) ? (512 - (size % 512)) : 0));
    }
  }

  // --------------------------------------------------------------------
  // Low-level helpers
  // --------------------------------------------------------------------

  private static boolean isAllZeros(byte[] buf) {
    for (byte b : buf) if (b != 0) return false;
    return true;
  }

  private static int readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
    int total = 0;
    while (total < len) {
      int r = in.read(buf, off + total, len - total);
      if (r == -1) return total == 0 ? -1 : total;
      total += r;
    }
    return total;
  }

  private static void copyFixedSize(InputStream in, OutputStream out, long size)
      throws IOException {
    byte[] buf = new byte[8192];
    long remaining = size;
    while (remaining > 0) {
      int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
      if (n == -1) break;
      out.write(buf, 0, n);
      remaining -= n;
    }
  }

  private static String getString(byte[] buf, int off, int len) {
    int end = off + len;
    int i = off;
    while (i < end && buf[i] != 0) i++;
    return new String(buf, off, i - off, java.nio.charset.StandardCharsets.US_ASCII).trim();
  }

  private static long parseOctal(byte[] buf, int off, int len) {
    int end = off + len;
    int i = off;

    while (i < end && (buf[i] == 0 || buf[i] == ' ')) i++;

    long value = 0;
    while (i < end && buf[i] >= '0' && buf[i] <= '7') {
      value = (value << 3) + (buf[i] - '0');
      i++;
    }
    return value;
  }
}
