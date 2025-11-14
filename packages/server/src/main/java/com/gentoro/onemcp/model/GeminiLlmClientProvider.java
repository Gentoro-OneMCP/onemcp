package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ConfigException;
import com.google.genai.Client;
import org.apache.commons.configuration2.Configuration;

/** SPI provider for Google Gemini-based {@link LlmClient} implementations. */
public final class GeminiLlmClientProvider implements LlmClientProvider {
  @Override
  public String providerId() {
    return "gemini";
  }

  @Override
  public LlmClient create(OneMcp oneMcp, Configuration subConfiguration) {
    String apiKey = subConfiguration.getString("apiKey");
    if (apiKey == null || apiKey.isBlank()) {
      throw new ConfigException("Missing llm.gemini.apiKey in configuration");
    }
    Client client = Client.builder().apiKey(apiKey).build();
    return new GeminiLlmClient(oneMcp, client, subConfiguration);
  }
}
