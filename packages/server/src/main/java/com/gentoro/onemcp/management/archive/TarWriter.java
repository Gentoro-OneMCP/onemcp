package com.gentoro.onemcp.management.archive;

import java.io.*;
import java.nio.file.*;
import java.util.zip.GZIPOutputStream;

/**
 * Utility for writing a directory tree as a gzip-compressed tar archive.
 *
 * <p>Only basic POSIX fields are populated (mode, uid, gid, mtime) and no extended headers.
 * Supports long paths using USTAR prefix rules.
 */
public final class TarWriter {

  private TarWriter() {}

  /** Write the directory at {@code root} as a .tar.gz stream. */
  public static void writeDirectoryAsTarGzip(Path root, OutputStream out) throws IOException {
    try (GZIPOutputStream gzos = new GZIPOutputStream(out)) {
      writeDirectoryAsTar(root, root, gzos);

      // End-of-archive: two 512-byte zero blocks
      byte[] zero = new byte[512];
      gzos.write(zero);
      gzos.write(zero);
    }
  }

  // --------------------------------------------------------------------
  // Recursive tar writing
  // --------------------------------------------------------------------

  private static void writeDirectoryAsTar(Path base, Path current, OutputStream out)
      throws IOException {

    if (Files.isDirectory(current)) {
      String name = normalizeTarName(base, current);
      if (!name.isEmpty()) {
        if (!name.endsWith("/")) name += "/";
        writeTarHeader(out, name, 0, (byte) '5');
      }

      try (DirectoryStream<Path> ds = Files.newDirectoryStream(current)) {
        for (Path child : ds) {
          writeDirectoryAsTar(base, child, out);
        }
      }
      return;
    }

    // Regular file
    String name = normalizeTarName(base, current);
    long size = Files.size(current);
    writeTarHeader(out, name, size, (byte) '0');

    try (InputStream in = Files.newInputStream(current)) {
      byte[] buf = new byte[8192];
      long remaining = size;
      while (remaining > 0) {
        int r = in.read(buf, 0, (int) Math.min(buf.length, remaining));
        if (r == -1) break;
        out.write(buf, 0, r);
        remaining -= r;
      }
    }

    // Padding to 512 bytes
    int pad = (int) (size % 512);
    if (pad != 0) out.write(new byte[512 - pad]);
  }

  // --------------------------------------------------------------------
  // Tar header construction
  // --------------------------------------------------------------------

  private static String normalizeTarName(Path base, Path path) {
    return base.relativize(path).toString().replace('\\', '/');
  }

  private static void writeTarHeader(OutputStream out, String name, long size, byte typeFlag)
      throws IOException {
    byte[] header = new byte[512];

    // Split path into prefix/name if needed
    String prefix = null;
    String n = name;
    if (n.length() > 100) {
      int idx = n.lastIndexOf('/');
      if (idx > 0 && idx <= 155 && (n.length() - idx - 1) <= 100) {
        prefix = n.substring(0, idx);
        n = n.substring(idx + 1);
      }
    }

    if (n.length() > 100) throw new IOException("Tar entry name too long: " + name);

    putAscii(header, 0, 100, n);

    putOctal(header, 100, 8, 0644); // mode
    putOctal(header, 108, 8, 0); // uid
    putOctal(header, 116, 8, 0); // gid
    putOctal(header, 124, 12, size);
    putOctal(header, 136, 12, System.currentTimeMillis() / 1000L);

    // Checksum placeholder (spaces)
    for (int i = 148; i < 156; i++) header[i] = ' ';

    header[156] = typeFlag;

    putAscii(header, 257, 6, "ustar");
    putAscii(header, 263, 2, "00");
    putAscii(header, 265, 32, "onemcp");
    putAscii(header, 297, 32, "onemcp");
    putOctal(header, 329, 8, 0);
    putOctal(header, 337, 8, 0);

    if (prefix != null) putAscii(header, 345, 155, prefix);

    long sum = 0;
    for (byte b : header) sum += (b & 0xFF);
    putOctal(header, 148, 8, sum);

    out.write(header);
  }

  private static void putAscii(byte[] buf, int off, int len, String s) {
    byte[] data = s.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(data, 0, buf, off, Math.min(len, data.length));
  }

  private static void putOctal(byte[] buf, int off, int len, long val) {
    String s = Long.toOctalString(val);
    int pad = len - 1 - s.length();
    for (int i = 0; i < pad; i++) buf[off + i] = '0';
    byte[] data = s.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(data, 0, buf, off + pad, data.length);
    buf[off + len - 1] = 0;
  }
}
