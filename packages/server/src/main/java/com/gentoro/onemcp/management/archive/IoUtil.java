package com.gentoro.onemcp.management.archive;

import com.gentoro.onemcp.exception.IoException;
import java.io.*;
import java.nio.file.*;

/** Small collection of I/O helpers for safe file operations. */
public final class IoUtil {

  private IoUtil() {}

  /** Copy an InputStream to a file path. */
  public static void copyStream(InputStream in, Path dest) throws IOException {
    Files.createDirectories(dest.getParent());
    try (OutputStream out = Files.newOutputStream(dest)) {
      in.transferTo(out);
    }
  }

  /**
   * Resolve a tar path securely inside a base directory.
   *
   * <p>Prevents ZipSlip attacks by normalizing and verifying parent directory containment.
   */
  public static Path secureResolve(Path base, String entryName) {
    Path dest = base.resolve(entryName).normalize();
    if (!dest.startsWith(base.normalize())) {
      throw new IoException("Blocked suspicious tar entry: " + entryName);
    }
    return dest;
  }

  /** Delete a directory tree quietly. */
  public static void silentDeleteDir(Path dir) {
    if (dir == null || !Files.exists(dir)) return;

    try (var stream = Files.walk(dir)) {
      stream
          .sorted((a, b) -> b.compareTo(a))
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (Exception ignored) {
                }
              });
    } catch (IOException ignored) {
    }
  }
}
