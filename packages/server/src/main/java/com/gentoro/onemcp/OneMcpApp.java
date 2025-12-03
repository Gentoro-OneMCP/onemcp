package com.gentoro.onemcp;

public class OneMcpApp {

  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OneMcpApp.class);

  public static void main(String[] args) {
    try {
      OneMcp app = new OneMcp(args);
      app.initialize();
      // Keep the server running until shutdown signal
      app.waitShutdownSignal();
    } catch (Exception e) {
      log.error("Application failed to start", e);
      System.exit(1);
    }
  }
}
