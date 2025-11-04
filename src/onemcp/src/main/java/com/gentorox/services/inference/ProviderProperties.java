package com.gentorox.services.inference;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Strongly-typed configuration properties backing inference provider selection.
 *
 * Expected configuration shape (application.yaml):
 *
 * providers:
 *   default-provider: openai
 *   providers:
 *     openai:
 *       api-key: ${OPENAI_API_KEY}
 *       base-url: https://api.openai.com/v1
 *       model-name: gpt-4o-mini
 *     anthropic:
 *       api-key: ${ANTHROPIC_API_KEY}
 *       model-name: claude-3-5-sonnet
 *     gemini:
 *       api-key: ${GOOGLE_API_KEY}
 *       model-name: gemini-1.5-pro
 *
 * Note: API keys are sensitive and must not be logged.
 */
@Component
@ConfigurationProperties(prefix = "providers")
public class ProviderProperties {

    /**
     * Map of provider-id -> settings for that provider (e.g., openai, anthropic, gemini).
     */
    private Map<String, ProviderSettings> providers;

    /**
     * The provider-id to use by default when creating the ChatLanguageModel.
     */
    private String defaultProvider;

    public Map<String, ProviderSettings> getProviders() {
        return providers;
    }

    public void setProviders(Map<String, ProviderSettings> providers) {
        this.providers = providers;
    }

    public String getDefaultProvider() {
        return defaultProvider;
    }

    public void setDefaultProvider(String defaultProvider) {
        this.defaultProvider = defaultProvider;
    }

    /**
     * Settings for a specific provider.
     */
    public static class ProviderSettings {
        /** Provider API key (keep secret). */
        private String apiKey;
        /** Optional base URL if pointing to a gateway or compatible server. */
        private String baseUrl;
        /** Optional endpoint path; not used for all providers. */
        private String endpoint;
        /** Model name identifier (e.g., gpt-4o-mini, claude-3-5-sonnet). */
        private String modelName;

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getModelName() {
            return modelName;
        }

        public void setModelName(String modelName) {
            this.modelName = modelName;
        }
    }
}
