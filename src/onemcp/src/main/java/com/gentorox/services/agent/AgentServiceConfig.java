package com.gentorox.services.agent;

import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.knowledgebase.KnowledgeBaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Spring configuration for creating and initializing the AgentService bean.
 * <p>
 * This configuration validates the configured foundation directory and initializes
 * the AgentService with the required dependencies.
 */
@Configuration
public class AgentServiceConfig {
  private static final Logger logger = LoggerFactory.getLogger(AgentServiceConfig.class);

  /**
   * Creates and initializes the AgentService bean.
   *
   * @param rootFoundationDir the root path of the knowledge base foundation directory. Can be configured via
   *                          the property "knowledgeBase.foundation.dir". Defaults to "/var/foundation".
   * @param inferenceService  the inference service dependency
   * @param kbService         the knowledge base service dependency
   * @return a fully initialized AgentService instance
   */
  @Bean
  public AgentService agentService(
      @Value("${knowledgeBase.foundation.dir:/var/foundation}") String rootFoundationDir,
      InferenceService inferenceService, KnowledgeBaseService kbService){

    Path rootFoundationPath = Path.of(rootFoundationDir);
    logger.info("Initializing AgentService with foundation dir: {}", rootFoundationPath.toAbsolutePath());
    if (!Files.exists(rootFoundationPath) || !Files.isDirectory(rootFoundationPath)) {
      throw new IllegalStateException("Foundation dir not found or not a directory: " + rootFoundationPath.toAbsolutePath());
    }

    AgentService agentService = new AgentService(inferenceService, kbService);
    agentService.initialize(rootFoundationPath);
    logger.info("AgentService initialized");
    return agentService;
  }
}
