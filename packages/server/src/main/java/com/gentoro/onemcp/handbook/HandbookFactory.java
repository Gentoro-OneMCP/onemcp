package com.gentoro.onemcp.handbook;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ConfigException;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.utility.FileUtility;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.Map;

public class HandbookFactory {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(HandbookFactory.class);

  public static Handbook create(OneMcp oneMcp) {

    String location = oneMcp.configuration().getString("handbook.location");
    if (location == null || location.isBlank()) {
      throw new ConfigException("Missing handbook.location config");
    }

    location = location.trim();
    log.trace("Resolving handbook location: {}", location);

    // Classpath-based handbook location
    if (location.startsWith("classpath:")) {
      String base = location.substring("classpath:".length());
      if (base.startsWith("/")) {
        log.warn("Classpath handbook location should not start with a slash: {}", location);
        base = base.substring(1);
      }
      if (base.startsWith("resources/")) {
        log.warn("Classpath handbook location should not start with 'resources/': {}", location);
        base = base.substring("resources/".length());
      }

      if (base.isBlank()) {
        throw new ConfigException("Invalid handbook.location: classpath base path is empty");
      }

      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) cl = HandbookFactory.class.getClassLoader();
      URL url = cl.getResource(base);
      if (url == null) {
        throw new ConfigException("Classpath handbook directory not found: " + location);
      }

      try {
        log.trace("Resolving classpath handbook location: {}", url);
        URI uri = url.toURI();
        Path sourcePath;
        FileSystem fs = null;
        boolean createdFs = false;
        try {
          sourcePath = Path.of(uri);
        } catch (FileSystemNotFoundException e) {
          // Likely a JAR resource, mount a new filesystem
          fs = FileSystems.newFileSystem(uri, Map.of());
          createdFs = true;
          sourcePath = Path.of(uri);
        } catch (FileSystemAlreadyExistsException e) {
          sourcePath = Path.of(uri);
        }

        if (!Files.isDirectory(sourcePath)) {
          throw new ConfigException("Classpath handbook path is not a directory: " + location);
        }

        // Copy classpath directory to a temporary writable directory so that generated files
        // (e.g., .services) can be written without mutating the classpath.
        Path tempDir = Files.createTempDirectory("handbook-");
        log.info(
            "Handbook is located on a readonly file system, copying classpath handbook directory to temporary directory ({})",
            tempDir);
        FileUtility.copyDirectory(sourcePath, tempDir);

        if (fs != null && createdFs) {
          try {
            fs.close();
          } catch (IOException ignored) {
          }
        }

        log.trace("Assigned handbook location: {}", tempDir);
        return new HandbookImpl(oneMcp, tempDir);
      } catch (URISyntaxException | IOException ex) {
        throw new HandbookException(
            "Failed to resolve classpath handbook directory: " + location, ex);
      }
    }

    // Filesystem-based location (absolute or relative path, or file: URI)
    Path basePath;
    try {
      if (location.startsWith("file:")) {
        basePath = Path.of(URI.create(location));
      } else {
        basePath = Path.of(location);
      }
    } catch (Exception iae) {
      throw new ConfigException("Invalid handbook.location URI/path: " + location, iae);
    }

    if (!Files.exists(basePath)) {
      throw new ConfigException("Handbook directory does not exist: " + basePath);
    }
    if (!Files.isDirectory(basePath)) {
      throw new ConfigException("Handbook path is not a directory: " + basePath);
    }

    log.trace("Assigned handbook location: {}", basePath);
    return new HandbookImpl(oneMcp, basePath);
  }
}
