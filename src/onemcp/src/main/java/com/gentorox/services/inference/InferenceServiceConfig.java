package com.gentorox.services.inference;

import com.gentorox.services.knowledgebase.KnowledgeBaseService;
import com.gentorox.services.telemetry.TelemetryService;
import com.gentorox.services.typescript.TypescriptRuntimeClient;
import com.gentorox.tools.AgentTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Spring configuration for wiring the InferenceService bean.
 *
 * This configuration centralizes the creation of the InferenceService, ensuring
 * a single instance is constructed with the currently configured ProviderProperties
 * and TelemetryService.
 */
@Configuration
public class InferenceServiceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(InferenceServiceConfig.class);

  /**
   * Create the InferenceService bean.
   *
   * Logs the default provider in use to help operators verify that the
   * application was configured as intended.
   *
   * @param providerProperties provider selection and per-provider settings
   * @param telemetry telemetry service for tracking
   * @return a configured InferenceService
   */
  @Bean
  InferenceService inferenceService(ApplicationContext applicationContext, ProviderProperties providerProperties, TelemetryService telemetry) {
    String defaultProvider = providerProperties != null ? providerProperties.getDefaultProvider() : null;
    LOGGER.info("Initializing InferenceService bean (defaultProvider={})", defaultProvider);
    return new InferenceService(applicationContext, providerProperties, telemetry);
  }

}
