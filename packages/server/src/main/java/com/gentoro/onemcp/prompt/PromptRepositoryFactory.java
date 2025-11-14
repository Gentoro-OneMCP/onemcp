package com.gentoro.onemcp.prompt;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.PromptException;
import com.gentoro.onemcp.prompt.impl.ClasspathPromptRepository;
import com.gentoro.onemcp.prompt.impl.FileSystemPromptRepository;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.configuration2.Configuration;

public class PromptRepositoryFactory {

  /**
   * Create a PromptRepository using the global OneMcp configuration. Looks for configuration under
   * the "prompt" namespace.
   */
  public static PromptRepository create(OneMcp oneMcp) {
    Configuration promptCfg = oneMcp.configuration().subset("prompt");
    return create(promptCfg);
  }

  /**
   * Create a PromptRepository using the provided configuration (usually a subset of "prompt").
   * Supported properties (in order of precedence): - location: "classpath:prompts" or
   * "file:/absolute/path" (or plain path for filesystem) - storage.dir: legacy filesystem directory
   * path
   */
  public static PromptRepository create(Configuration promptCfg) {
    if (promptCfg == null) {
      throw new IllegalArgumentException("Prompt configuration not provided");
    }

    String location = promptCfg.getString("location");
    if (location == null || location.isBlank()) {
      throw new IllegalArgumentException(
          "Missing prompt.location or prompt.location configuration");
    }

    location = location.trim();

    // Classpath-based repository
    if (location.startsWith("classpath:")) {
      String base = location.substring("classpath:".length());
      if (base.startsWith("/")) base = base.substring(1);
      if (base.isBlank()) {
        throw new IllegalArgumentException("Invalid prompt.classpath location: base path is empty");
      }
      return new ClasspathPromptRepository(base);
    }

    // Filesystem-based repository (file: or plain path)
    Path basePath;
    try {
      if (location.startsWith("file:")) {
        basePath = Path.of(URI.create(location));
      } else {
        basePath = Path.of(location);
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("Invalid prompt location URI/path: " + location, iae);
    }

    if (!Files.exists(basePath)) {
      throw new IllegalArgumentException("Prompt storage directory does not exist: " + basePath);
    }
    if (!Files.isDirectory(basePath)) {
      throw new IllegalArgumentException("Prompt storage path is not a directory: " + basePath);
    }

    try {
      return new FileSystemPromptRepository(basePath);
    } catch (IOException e) {
      throw new PromptException("Failed to initialize filesystem prompt repository", e);
    }
  }
}
