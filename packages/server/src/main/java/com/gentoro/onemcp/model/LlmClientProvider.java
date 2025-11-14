package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import org.apache.commons.configuration2.Configuration;

/**
 * Service Provider Interface (SPI) for pluggable LLM providers.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}. Each provider identifies
 * itself with a stable {@code providerId} (e.g. "openai", "anthropic", "ollama", "mlx", "gemini").
 * The factory will select an implementation by matching the user configuration to this id.
 *
 * <p>To register a provider, add its fully qualified class name to the service resource: {@code
 * META-INF/services/com.gentoro.onemcp.model.LlmClientProvider}.
 */
public interface LlmClientProvider {

  /** A stable, lowercase identifier for this provider (e.g. "openai"). */
  String providerId();

  /**
   * Creates a configured {@link LlmClient} instance.
   *
   * <p>Implementations should validate required configuration keys and provide clear error messages
   * when missing (e.g. API keys). This method must be side-effect free beyond constructing the
   * client instance.
   *
   * @param subConfiguration provider-specific configuration subset (e.g. {@code llm.openai.*}).
   * @return a configured client.
   * @throws IllegalArgumentException when the configuration is invalid.
   */
  LlmClient create(OneMcp oneMcp, Configuration subConfiguration);
}
