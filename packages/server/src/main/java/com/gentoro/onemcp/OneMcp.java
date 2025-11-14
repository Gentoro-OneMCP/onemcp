package com.gentoro.onemcp;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import com.gentoro.onemcp.acme.AcmeServer;
import com.gentoro.onemcp.actuator.ActuatorService;
import com.gentoro.onemcp.context.KnowledgeBase;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.KnowledgeBaseException;
import com.gentoro.onemcp.exception.StateException;
import com.gentoro.onemcp.http.EmbeddedJettyServer;
import com.gentoro.onemcp.mcp.McpServer;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.model.LlmClientFactory;
import com.gentoro.onemcp.orchestrator.OrchestratorService;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptRepositoryFactory;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.commons.configuration2.Configuration;

public class OneMcp {

  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OneMcp.class);

  private final StartupParameters startupParameters;
  private ConfigurationProvider configurationProvider;
  private PromptRepository promptRepository;
  private EmbeddedJettyServer httpServer;
  private KnowledgeBase knowledgeBase;
  private LlmClient llmClient;
  private OrchestratorService orchestrator;

  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private volatile Thread shutdownHook;

  public OneMcp(String[] applicationArgs) {
    this.startupParameters = new StartupParameters(applicationArgs);
  }

  public boolean isInteractiveModeEnabled() {
    return "interactive".equalsIgnoreCase(startupParameters().getParameter("mode", String.class));
  }

  public void initialize() {
    // Disable java logging entirely.
    LogManager.getLogManager().reset();
    Logger.getLogger("").setLevel(Level.OFF);

    this.configurationProvider = new ConfigurationProvider(startupParameters.configFile());
    // Apply logging levels from application.yaml as early as possible
    com.gentoro.onemcp.logging.LoggingService.applyConfiguration(configuration());
    this.promptRepository = PromptRepositoryFactory.create(this);

    try {
      this.knowledgeBase = new KnowledgeBase(this);
      this.knowledgeBase.ingestHandbook();
    } catch (Exception e) {
      throw new KnowledgeBaseException("Failed to ingest Handbook content", e);
    }

    this.llmClient = LlmClientFactory.createProvider(this);

    // Initialize shared Jetty server and register components
    this.httpServer = new EmbeddedJettyServer(this);
    httpServer.prepare();

    try {
      // Register ACME analytics API
      new AcmeServer(this).register();
      // Register actuator health endpoint
      new ActuatorService(this).register();
      // Register MCP servlet
      new McpServer(this).register();

      // Start Jetty (non-blocking)
      httpServer.start();
    } catch (Exception e) {
      shutdown();
      throw new ExecutionException("Could not start http server", e);
    }

    this.orchestrator = new OrchestratorService(this);

    // If running in interactive mode: disable STDOUT appender and enable file-based logging
    String mode = startupParameters().getParameter("mode", String.class);
    if ("interactive".equalsIgnoreCase(mode)) {
      configureFileOnlyLogging();
    }

    switch (startupParameters().getParameter("mode", String.class)) {
      case "interactive":
        this.orchestrator.enterInteractiveMode();
        break;
      case "dry-run":
        this.orchestrator.handlePrompt("test");
        break;
      case "server":
        // server is always started
        break;
      default:
        shutdown();
        throw new IllegalArgumentException(
            "Invalid mode: " + startupParameters().getParameter("mode", String.class));
    }
  }

  /**
   * Block the current thread until a shutdown signal is received (e.g., Ctrl+C or JVM termination).
   * When signaled, this method invokes {@link #shutdown()} to release resources before returning.
   */
  public void waitShutdownSignal() {
    // Register a JVM shutdown hook once
    if (shutdownHook == null) {
      synchronized (this) {
        if (shutdownHook == null) {
          shutdownHook = new Thread(this::shutdown, "onemcp-shutdown-hook");
          Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
      }
    }

    // Wait until shutdown is triggered
    try {
      shutdownLatch.await();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /** Release resources. Safe to call multiple times; executed only once. */
  public void shutdown() {
    triggerShutdown("explicit");
  }

  private void closeQuietly(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception ignored) {
      }
    }
  }

  private void triggerShutdown(String reason) {
    if (shuttingDown.compareAndSet(false, true)) {
      try {
        closeQuietly(httpServer);
      } finally {
        shutdownLatch.countDown();
      }
    }
  }

  /** Expose the application configuration to other components. */
  public Configuration configuration() {
    if (configurationProvider == null) {
      throw new StateException("OneMcp not initialized. Call initialize() first.");
    }
    return configurationProvider.config();
  }

  public StartupParameters startupParameters() {
    return startupParameters;
  }

  public PromptRepository promptRepository() {
    return promptRepository;
  }

  public EmbeddedJettyServer httpServer() {
    return httpServer;
  }

  public KnowledgeBase knowledgeBase() {
    return knowledgeBase;
  }

  public LlmClient llmClient() {
    return llmClient;
  }

  public OrchestratorService orchestrator() {
    return orchestrator;
  }

  /**
   * Reconfigure Logback to disable console output and enable only file-based logging. Intended for
   * use in "interactive" mode to keep console clean.
   */
  private void configureFileOnlyLogging() {
    LoggerContext context = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
    ch.qos.logback.classic.Logger root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);

    // Detach any console appenders (e.g., STDOUT)
    for (java.util.Iterator<Appender<ILoggingEvent>> it = root.iteratorForAppenders();
        it.hasNext(); ) {
      Appender<ILoggingEvent> app = it.next();
      if (app instanceof ConsoleAppender) {
        root.detachAppender(app);
      }
    }

    // Ensure logs directory exists
    File logsDir = new File("logs");
    if (!logsDir.exists()) {
      // noinspection ResultOfMethodCallIgnored
      logsDir.mkdirs();
    }

    // Configure rolling file appender
    RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
    fileAppender.setContext(context);
    fileAppender.setName("FILE");
    fileAppender.setFile(new File(logsDir, "onemcp.log").getPath());

    TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
    rollingPolicy.setContext(context);
    rollingPolicy.setParent(fileAppender);
    rollingPolicy.setFileNamePattern(new File(logsDir, "onemcp.%d{yyyy-MM-dd}.log.gz").getPath());
    rollingPolicy.setMaxHistory(7);
    rollingPolicy.start();

    PatternLayoutEncoder encoder = new PatternLayoutEncoder();
    encoder.setContext(context);
    encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
    encoder.start();

    fileAppender.setEncoder(encoder);
    fileAppender.setRollingPolicy(rollingPolicy);
    fileAppender.start();

    root.addAppender(fileAppender);
    log.info(
        "Interactive mode: console logging disabled; file logging enabled at {}",
        new File(logsDir, "onemcp.log").getPath());
  }
}
