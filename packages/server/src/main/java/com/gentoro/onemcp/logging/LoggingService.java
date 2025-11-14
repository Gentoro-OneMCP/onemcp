package com.gentoro.onemcp.logging;

import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Central place to obtain SLF4J loggers and extend logging concerns later (MDC, markers, etc.). */
public final class LoggingService {
  private static final Logger log = LoggerFactory.getLogger(LoggingService.class);

  private LoggingService() {}

  public static Logger getLogger(Class<?> clazz) {
    return LoggerFactory.getLogger(clazz);
  }

  /**
   * Apply logging levels from application configuration.
   *
   * <p>Expected YAML structure: logging: level: root: INFO com.gentoro.onemcp: DEBUG
   * org.eclipse.jetty: WARN
   */
  public static void applyConfiguration(Configuration cfg) {
    if (cfg == null) return;
    try {
      ch.qos.logback.classic.LoggerContext ctx =
          (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();

      // Root level
      String rootLvl = cfg.getString("logging.level.root", null);
      if (rootLvl != null && !rootLvl.isBlank()) {
        setLevel(ctx.getLogger(Logger.ROOT_LOGGER_NAME), rootLvl);
      }

      // Package/class-specific levels
      Configuration levels = cfg.subset("logging.level");
      if (levels != null) {
        java.util.Iterator<String> it = levels.getKeys();
        while (it.hasNext()) {
          String key = it.next();
          if ("root".equalsIgnoreCase(key)) continue;
          String lvl = levels.getString(key, null);
          if (lvl == null || lvl.isBlank()) continue;
          setLevel(ctx.getLogger(key), lvl);
        }
      }
    } catch (Exception e) {
      log.warn(
          "Failed to apply logging configuration from YAML; falling back to logback.xml settings",
          e);
    }
  }

  private static void setLevel(ch.qos.logback.classic.Logger logger, String levelStr) {
    if (logger == null || levelStr == null) return;
    ch.qos.logback.classic.Level level =
        ch.qos.logback.classic.Level.toLevel(levelStr.trim(), null);
    if (level == null) {
      log.warn("Unknown log level '{}'; ignoring for logger {}", levelStr, logger.getName());
      return;
    }
    logger.setLevel(level);
    log.debug("Set logger '{}' to level {}", logger.getName(), level);
  }
}
