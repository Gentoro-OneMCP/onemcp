package com.gentoro.onemcp.management.archive;

import java.io.InputStream;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * Computes SHA-256 hashes of files within a directory tree.
 *
 * <p>Used for verifying client-side in sync state.
 */
public final class HashUtil {

  private HashUtil() {}

  /**
   * Recursively walk a directory and compute SHA-256 for all files.
   *
   * @return map of relative POSIX file paths → lowercase hex digest
   */
  public static Map<String, String> computeDirectoryHashes(Path root) {
    Map<String, String> result = new HashMap<>();

    try (var stream = Files.walk(root)) {
      for (Path p : (Iterable<Path>) stream::iterator) {
        if (!Files.isRegularFile(p)) continue;

        String rel = root.relativize(p).toString().replace('\\', '/');
        result.put(rel, sha256Hex(p));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed computing hashes: " + root, e);
    }

    return result;
  }

  private static String sha256Hex(Path file) {
    try (InputStream in = Files.newInputStream(file)) {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] buf = new byte[8192];

      int n;
      while ((n = in.read(buf)) != -1) md.update(buf, 0, n);

      StringBuilder sb = new StringBuilder();
      for (byte b : md.digest()) sb.append(String.format("%02x", b));
      return sb.toString();

    } catch (Exception e) {
      throw new RuntimeException("Failed hashing file: " + file, e);
    }
  }
}
