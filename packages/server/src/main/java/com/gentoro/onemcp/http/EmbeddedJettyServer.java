package com.gentoro.onemcp.http;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ConfigException;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.NetworkException;
import java.util.Objects;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Embedded Jetty 12 server with a root {@link ServletContextHandler} and a JVM shutdown hook.
 *
 * <p>This class owns the Jetty lifecycle (start/stop/join) and exposes the underlying {@link
 * Server} and {@link ServletContextHandler} so that other components can register their
 * servlets/endpoints.
 */
public class EmbeddedJettyServer implements AutoCloseable {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(EmbeddedJettyServer.class);
  private final OneMcp oneMcp;
  private final Object lifecycleLock = new Object();
  private Server server;
  private ServletContextHandler contextHandler;

  public EmbeddedJettyServer(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /** Prepare the Jetty Server and root ServletContextHandler without starting it. */
  public void prepare() {
    log.trace("Initializing shared Jetty server");
    synchronized (lifecycleLock) {
      if (server != null) {
        log.trace("Server already prepared");
        return;
      }

      int port = 0;
      try {
        port = oneMcp.configuration().getInt("http.port", 8080);
        log.trace("Resolving http.port: {}", port);
      } catch (Exception e) {
        throw new ConfigException("Failed to resolve http.port configuration", e);
      }

      String hostname = "0.0.0.0";
      try {
        hostname = oneMcp.configuration().getString("http.hostname", "0.0.0.0");
        if (Objects.isNull(hostname) || hostname.isBlank()) {
          throw new ConfigException("Missing http.hostname configuration");
        }
        hostname = hostname.trim();
        log.trace("Resolving http.hostname: {}", hostname);
      } catch (Exception e) {
        throw ExceptionUtil.rethrowIfUnchecked(
            e, (ex) -> new ConfigException("Failed to resolve http.hostname configuration", ex));
      }

      try {
        if (!hostname.equals("0.0.0.0")) {
          // Create connector and specify host + port
          ServerConnector connector = new ServerConnector(server);
          connector.setHost(hostname);
          connector.setPort(port);
          server = new Server();
          server.addConnector(connector);
        } else {
          server = new Server(port);
        }

        contextHandler = new ServletContextHandler();
        contextHandler.setContextPath("/");
        server.setHandler(contextHandler);
      } catch (Exception e) {
        throw new NetworkException(
            "There was a problem while attempting to initialize jetty service. "
                + "Please, check if the chosen port and hostname are available and that this process has the proper permission to start a new listener on these configurations",
            e);
      }
    }
  }

  /**
   * Start Jetty if not already started. The port is resolved from configuration keys: - http.port
   * (preferred) - mcp.http.port (fallback) Default: 8080
   */
  public void start() throws Exception {
    log.trace("Starting shared Jetty server");
    synchronized (lifecycleLock) {
      if (server != null && server.isStarted()) {
        log.trace("Server already started");
        return;
      }

      if (server == null) {
        log.warn("Called start() before prepare()");
        prepare();
      }

      try {
        int port = getPort();
        log.info("Starting shared Jetty server on port {}...", port);
        server.start();
        log.info("Jetty listening on http://localhost:{}", getPort());
      } catch (Exception e) {
        throw ExceptionUtil.rethrowIfUnchecked(
            e,
            (ex) ->
                new NetworkException(
                    "There was a problem while attempting to start jetty service. "
                        + "Please, check if the chosen port and hostname are available and that this process has the proper permission to start a new listener on these configurations",
                    ex));
      }
    }
  }

  public void stop() throws Exception {
    log.trace("Stopping shared Jetty server");
    synchronized (lifecycleLock) {
      if (server != null) {
        try {
          try {
            if (server.isRunning() || server.isStarted() || server.isStarting()) {
              server.stop();
            }
          } catch (Exception e) {
            log.error(
                "Error stopping jetty server, as not to prevent other services from stopping, exception was captured and logged, but bot rethrown downstream.",
                e);
          }
        } finally {
          server = null;
          contextHandler = null;
        }
      }
    }
  }

  public void join() throws InterruptedException {
    Server s;
    synchronized (lifecycleLock) {
      s = this.server;
    }
    if (s != null) s.join();
  }

  public boolean isRunning() {
    synchronized (lifecycleLock) {
      return server != null && server.isRunning();
    }
  }

  public int getPort() {
    synchronized (lifecycleLock) {
      if (server != null && server.isStarted()) {
        return server.getURI().getPort();
      }
      return oneMcp.configuration().getInt("http.port", 8080);
    }
  }

  public Server getServer() {
    synchronized (lifecycleLock) {
      return server;
    }
  }

  public ServletContextHandler getContextHandler() {
    synchronized (lifecycleLock) {
      return contextHandler;
    }
  }

  @Override
  public void close() throws Exception {
    stop();
  }
}
