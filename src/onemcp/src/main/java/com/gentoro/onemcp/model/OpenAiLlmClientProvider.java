package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import org.apache.commons.configuration2.Configuration;

/** SPI provider for OpenAI-based {@link LlmClient} implementations. */
public final class OpenAiLlmClientProvider implements LlmClientProvider {

  @Override
  public String providerId() {
    return "openai";
  }

  @Override
  public LlmClient create(OneMcp oneMcp, Configuration subConfiguration) {
    String apiKey = subConfiguration.getString("apiKey");
    if (apiKey == null || apiKey.isBlank()) {
      throw new IllegalArgumentException("Missing llm.openai.apiKey in configuration");
    }
    OpenAIClient client = OpenAIOkHttpClient.builder().apiKey(apiKey).build();
    return new OpenAiLlmClient(oneMcp, client, subConfiguration);
  }
}
