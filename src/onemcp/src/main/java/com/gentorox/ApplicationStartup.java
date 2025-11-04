package com.gentorox;

import com.gentorox.services.indexer.ValidationRunner;
import com.gentorox.services.regression.RegressionReport;
import com.gentorox.services.regression.RegressionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Boots optional processes on application startup based on the `--process` argument.
 */
@Component
public class ApplicationStartup implements ApplicationRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationStartup.class);

  private final RegressionService regressionService;

  public ApplicationStartup(RegressionService regressionService) {
    this.regressionService = Objects.requireNonNull(regressionService, "regressionService");
  }

  @Override
  public void run(ApplicationArguments args) {
    String process = args.containsOption("process") ? args.getOptionValues("process").get(0) : "standard";
    switch (process) {
      case "validate" -> ValidationRunner.run();
      case "regression" -> {
        LOG.info("Starting regression process...");
        RegressionReport report = regressionService.runAll();
        int exitCode = report.failedCount() > 0 ? 1 : 0;
        LOG.info("Regression process finished with exit code {}", exitCode);
        System.exit(exitCode);
      }
      default -> LOG.info("Starting standard mode (MCP server + orchestrator)");
    }
  }
}
