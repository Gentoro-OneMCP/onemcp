package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import org.apache.commons.configuration2.Configuration;

/** SPI provider for Ollama-based {@link LlmClient} implementations. */
public final class OllamaLlmClientProvider implements LlmClientProvider {
  @Override
  public String providerId() {
    return "ollama";
  }

  @Override
  public LlmClient create(OneMcp oneMcp, Configuration subConfiguration) {
    // baseUrl and model are read within OllamaLlmClient; provide sane defaults.
    return new OllamaLlmClient(oneMcp, subConfiguration);
  }
}
