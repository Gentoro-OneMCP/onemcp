package com.gentorox.services.knowledgebase;

import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.typescript.TypescriptRuntimeClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Spring configuration for the Knowledge Base service.
 *
 * Properties:
 * - knowledgeBase.foundation.dir: The root directory containing docs, tests, feedback, and openapi subfolders.
 * - knowledgeBase.hint.useAi: Whether to use the InferenceService to generate short human-friendly hints for entries.
 * - knowledgeBase.hint.size: Maximum length of the generated hint.
 */
@Configuration
public class KnowledgeBaseConfig {
  private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseConfig.class);

  /**
   * Builds and initializes the KnowledgeBaseService at startup.
   */
  @Bean
  public KnowledgeBaseService knowledgeBaseService(
                               @Value("${knowledgeBase.foundation.dir:/var/foundation}") String rootFoundationDir,
                               @Value("${knowledgeBase.hint.useAi:false}") boolean hintAiGenerationEnabled,
                               @Value("${knowledgeBase.hint.size:240}") int hintContentLimit,
                               InferenceService inferenceService,
                              TypescriptRuntimeClient tsRuntimeClient,
                              KnowledgeBasePersistence persistence) throws IOException {
    Path rootFoundationPath = Path.of(rootFoundationDir);
    logger.info("Initializing KnowledgeBaseService with foundation dir: {}", rootFoundationPath.toAbsolutePath());
    if (!Files.exists(rootFoundationPath) || !Files.isDirectory(rootFoundationPath)) {
      throw new IllegalStateException("Foundation dir not found: " + rootFoundationPath.toAbsolutePath());
    }

    Path stateDir = rootFoundationPath.resolve("state");
    if (!Files.exists(stateDir)) {
      Files.createDirectories(stateDir);
      logger.debug("Created state directory at {}", stateDir.toAbsolutePath());
    }

    Path stateFile = stateDir.resolve("knowledge-base-state.json");
    logger.info("KnowledgeBase hints: aiGenerationEnabled={}, maxLength={}", hintAiGenerationEnabled, hintContentLimit);
    KnowledgeBaseService knowledgeBaseService = new KnowledgeBaseServiceImpl(inferenceService, tsRuntimeClient, persistence, stateFile, hintAiGenerationEnabled, hintContentLimit);
    knowledgeBaseService.initialize(Path.of(rootFoundationDir));
    logger.info("KnowledgeBaseService initialized; state file: {}", stateFile.toAbsolutePath());
    return knowledgeBaseService;
  }

}
