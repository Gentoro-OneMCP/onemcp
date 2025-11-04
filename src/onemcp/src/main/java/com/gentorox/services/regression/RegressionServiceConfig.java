package com.gentorox.services.regression;

import com.gentorox.services.agent.Orchestrator;
import com.gentorox.services.inference.InferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class RegressionServiceConfig {
  private static final Logger logger = LoggerFactory.getLogger(RegressionServiceConfig.class);

  @Bean
  public RegressionService regressionService(@Value("${knowledgeBase.foundation.dir:/var/foundation}") String rootFoundationDir, Orchestrator orchestrator, InferenceService inferenceService) {
    Path rootFoundationPath = Path.of(rootFoundationDir);
    logger.info("Initializing KnowledgeBaseService with foundation dir: {}", rootFoundationPath.toAbsolutePath());
    if (!Files.exists(rootFoundationPath) || !Files.isDirectory(rootFoundationPath)) {
      throw new IllegalStateException("Foundation dir not found: " + rootFoundationPath.toAbsolutePath());
    }

    return new RegressionService(rootFoundationPath, orchestrator, inferenceService);
  }

}
