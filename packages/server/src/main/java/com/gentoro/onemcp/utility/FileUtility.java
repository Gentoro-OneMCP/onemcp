package com.gentoro.onemcp.utility;

import com.gentoro.onemcp.exception.IoException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;

public class FileUtility {

  public static void deleteDir(Path dir, boolean quietly) {
    try {
      if (Files.exists(dir)) {
        try (var paths = Files.walk(dir)) {
          paths
              .sorted(Comparator.reverseOrder()) // delete children first
              .forEach(
                  path -> {
                    try {
                      Files.delete(path);
                    } catch (IOException e) {
                      throw new IoException("Failed to delete file: " + path, e);
                    }
                  });
        }
      }
    } catch (Exception e) {
      if (!quietly) {
        throw new IoException("Failed to delete directory: " + dir, e);
      }
    }
  }

  public static void copyDirectory(Path source, Path target) {
    try {
      try (var stream = Files.walk(source)) {
        for (Path p : (Iterable<Path>) stream::iterator) {
          Path rel = source.relativize(p);
          Path dest = target.resolve(rel.toString());
          if (Files.isDirectory(p)) {
            Files.createDirectories(dest);
          } else {
            if (dest.getParent() != null) Files.createDirectories(dest.getParent());
            Files.copy(p, dest, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    } catch (IOException e) {
      throw new IoException("Failed to copy directory: " + source + " to: " + target, e);
    }
  }
}
