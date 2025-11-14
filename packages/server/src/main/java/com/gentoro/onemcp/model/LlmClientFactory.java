package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import java.util.Objects;
import java.util.ServiceLoader;
import org.apache.commons.configuration2.Configuration;

/**
 * Factory utility to create {@link LlmClient} instances from configuration.
 *
 * <p>Preferred resolution uses Java's {@link ServiceLoader} to locate a matching {@link
 * LlmClientProvider} by {@code providerId()}. For backward compatibility, if no SPI provider
 * matches, the legacy in-code switch is used.
 */
public final class LlmClientFactory {
  private LlmClientFactory() {}

  /**
   * Creates a client using an indirection key under the {@code llm.*} namespace.
   *
   * <p>Example configuration:
   *
   * <pre>
   *   llm.selected = openai
   *   llm.openai.provider = openai
   *   llm.openai.apiKey = sk-...
   * </pre>
   *
   * @param oneMcp application context instance
   * @return configured client instance
   */
  public static LlmClient createProvider(OneMcp oneMcp) {
    String namespace = oneMcp.configuration().getString("llm.active-profile", "default").trim();
    if (namespace.isEmpty()
        || !oneMcp.configuration().getKeys("llm.%s".formatted(namespace)).hasNext()) {
      throw new IllegalArgumentException("Missing llm.%s configuration".formatted(namespace));
    }
    Configuration subConfig = oneMcp.configuration().subset("llm.%s".formatted(namespace));
    return create(oneMcp, subConfig);
  }

  /**
   * Creates a client from a provider-specific subset configuration.
   *
   * <p>Expected keys include at least {@code provider} and any provider-specific settings (e.g.
   * {@code apiKey}, {@code baseUrl}, {@code model}).
   */
  public static LlmClient create(OneMcp oneMcp, Configuration subConfig) {
    String provider =
        Objects.requireNonNull(subConfig.getString("provider"), "llm.<ns>.provider")
            .trim()
            .toLowerCase();

    // Try SPI providers first
    for (LlmClientProvider p : ServiceLoader.load(LlmClientProvider.class)) {
      if (provider.equals(p.providerId())) {
        return p.create(oneMcp, subConfig);
      }
    }

    throw new IllegalArgumentException(
        "Unknown llm.%s.provider: %s".formatted(provider, subConfig));
  }
}
