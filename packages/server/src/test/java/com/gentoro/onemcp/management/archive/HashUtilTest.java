package com.gentoro.onemcp.management.archive;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class HashUtilTest {

  @TempDir Path temp;

  @Test
  void computesHashesForFiles() throws Exception {
    Path dir = temp.resolve("root");
    Files.createDirectories(dir);

    Path a = dir.resolve("a.txt");
    Files.writeString(a, "abc");

    Path b = dir.resolve("nested/b.txt");
    Files.createDirectories(b.getParent());
    Files.writeString(b, "123");

    Map<String, String> hashes = HashUtil.computeDirectoryHashes(dir);

    assertEquals(2, hashes.size());
    assertTrue(hashes.containsKey("a.txt"));
    assertTrue(hashes.containsKey("nested/b.txt"));
    assertEquals(64, hashes.get("a.txt").length());
  }
}
