package com.gentoro.onemcp;

import com.gentoro.onemcp.exception.ConfigException;
import com.gentoro.onemcp.exception.SerializationException;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.interpol.ConfigurationInterpolator;
import org.apache.commons.configuration2.interpol.Lookup;

/**
 * Loads YAML configuration and exposes an Apache Commons Configuration instance.
 *
 * <p>Default usage historically relied on a singleton reading "application.yaml" from the
 * classpath. For better testability and flexibility, an instance-based constructor that accepts a
 * location string is also provided. Location formats supported: - "classpath:some/path.yaml"
 * (loaded from the application classpath) - Absolute or relative filesystem path (e.g.,
 * "/etc/app.yaml" or "config/app.yaml")
 */
public final class ConfigurationProvider {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ConfigurationProvider.class);
  private static volatile ConfigurationProvider INSTANCE;
  private final Configuration configuration;

  /** Legacy singleton uses classpath resource "application.yaml". */
  private ConfigurationProvider() {
    this.configuration = loadYamlFromClasspath("application.yaml");
  }

  /** Preferred instance-based constructor using a location string. */
  public ConfigurationProvider(String location) {
    this.configuration = loadYamlFromLocation(location);
  }

  /** Access to raw Commons Configuration object. */
  public Configuration config() {
    return configuration;
  }

  private static Configuration loadYamlFromClasspath(String resourceName) {
    URL resourceUrl = Thread.currentThread().getContextClassLoader().getResource(resourceName);
    if (resourceUrl == null) {
      // Fallback to empty YAML config if not found; avoids NPEs and allows defaults.
      return addOns(new YAMLConfiguration());
    }
    log.info("Loading configuration from classpath resource: {}", resourceName);
    try (InputStream input =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName); ) {
      if (input == null) {
        throw new FileNotFoundException("Resource not found: %s".formatted(resourceName));
      }
      String yamlContent = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      YAMLConfiguration config = new YAMLConfiguration();
      config.read(new StringReader(yamlContent));
      return addOns(config);
    } catch (Exception e) {
      throw new SerializationException(
          "Failed to read YAML from classpath resource: " + resourceName, e);
    }
  }

  private static Configuration loadYamlFromFile(File file) {
    try {
      Parameters params = new Parameters();
      FileBasedConfigurationBuilder<YAMLConfiguration> builder =
          new FileBasedConfigurationBuilder<>(YAMLConfiguration.class)
              .configure(params.fileBased().setFile(file));
      return addOns(builder.getConfiguration());
    } catch (ConfigurationException e) {
      throw new ConfigException("Failed to load YAML file: " + file, e);
    }
  }

  private static Configuration loadYamlFromLocation(String location) {
    if (location == null || location.isBlank()) {
      // Default to classpath:application.yaml
      return loadYamlFromClasspath("application.yaml");
    }
    String loc = location.trim();
    if (loc.startsWith("classpath:")) {
      return loadYamlFromClasspath(loc.substring("classpath:".length()));
    }
    // Try to interpret as URI first (e.g., file:/etc/app.yaml)
    try {
      URI uri = URI.create(loc);
      if (uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("file")) {
        return loadYamlFromFile(new File(uri));
      }
    } catch (IllegalArgumentException ignored) {
      // fall back to plain file path
    }
    // Treat as file system path (relative or absolute)
    return loadYamlFromFile(new File(loc));
  }

  private static Configuration addOns(Configuration config) {
    ConfigurationInterpolator interpolator = config.getInterpolator();
    interpolator.registerLookup("env", new FallbackEnvLookup());
    return config;
  }

  private static class FallbackEnvLookup implements Lookup {
    private Map<String, String> fallback = null;

    public FallbackEnvLookup() {}

    @Override
    public Object lookup(String key) {
      // 1. Real environment value
      String val = System.getenv(key);
      if (val != null && !val.isEmpty()) {
        return val;
      }

      if (fallback == null) {
        synchronized (this) {
          if (fallback == null) {
            Path path = findEnvFile();
            if (path == null) {
              log.warn(
                  "Could not locate .env.local anywhere, skipping environment variables auto-detection.");
              this.fallback = new HashMap<>();
            } else {
              this.fallback = readKeyValueFile(path);
            }
          }
        }
      }

      // 2. Fallback (your own env map)
      return fallback.get(key);
    }

    private Path findEnvFile() {
      // 1) CWD
      Path p1 = Paths.get(".env.local");
      if (Files.exists(p1)) {
        return p1;
      } else {
        log.info("No .env.local found in CWD {}", p1.toAbsolutePath());
      }
      // 2) Module-relative when running from repo root
      Path p2 = Paths.get("packages/server/.env.local");
      if (Files.exists(p2)) {
        return p2;
      } else {
        log.info("No .env.local found in CWD {}", p2.toAbsolutePath());
      }
      return null;
    }

    private Map<String, String> readKeyValueFile(Path path) {
      log.info("Reading .env.local file: {}", path.toAbsolutePath());
      try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
        return br.lines()
            .map(String::trim)
            .filter(line -> !line.isEmpty())
            .filter(line -> !line.startsWith("#"))
            .map(this::parseLine)
            .filter(e -> e.getKey() != null && !e.getKey().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b));
      } catch (IOException e) {
        return Collections.emptyMap();
      }
    }

    private Map.Entry<String, String> parseLine(String line) {
      int idx = line.indexOf('=');
      if (idx <= 0) return Map.entry("", "");
      String key = line.substring(0, idx).trim();
      String rawVal = line.substring(idx + 1).trim();
      String val = rawVal;
      if ((val.startsWith("\"") && val.endsWith("\""))
          || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.substring(1, val.length() - 1);
      }
      return Map.entry(key, val);
    }
  }
}
