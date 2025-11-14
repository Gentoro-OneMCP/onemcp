package com.gentoro.onemcp.model;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.gentoro.onemcp.OneMcp;
import org.apache.commons.configuration2.Configuration;

/** SPI provider for Anthropic-based {@link LlmClient} implementations. */
public final class AnthropicLlmClientProvider implements LlmClientProvider {
  @Override
  public String providerId() {
    return "anthropic";
  }

  @Override
  public LlmClient create(OneMcp oneMcp, Configuration configuration) {
    String apiKey = configuration.getString("apiKey");
    if (apiKey == null || apiKey.isBlank()) {
      throw new IllegalArgumentException("Missing llm.anthropic.apiKey in configuration");
    }
    AnthropicClient client = AnthropicOkHttpClient.builder().apiKey(apiKey).build();
    return new AnthropicLlmClient(oneMcp, client, configuration);
  }
}
