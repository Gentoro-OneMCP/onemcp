package com.gentoro.onemcp;

import com.gentoro.onemcp.exception.ConfigException;
import com.gentoro.onemcp.exception.SerializationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

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
      return new YAMLConfiguration();
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
      return config;
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
      return builder.getConfiguration();
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
}
