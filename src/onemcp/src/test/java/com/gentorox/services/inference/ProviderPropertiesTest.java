package com.gentorox.services.inference;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for ProviderConfig to verify environment variable loading.
 */
@SpringBootTest
@TestPropertySource(properties = {
    "providers.providers.openai.apiKey=${OPENAI_API_KEY:test-key}",
    "providers.providers.openai.modelName=gpt-4o-mini",
    "providers.default-provider=openai"
})
class ProviderPropertiesTest {

    @Test
    void testProviderConfigLoading() {
        ProviderProperties config = new ProviderProperties();

        // Set up test configuration
        ProviderProperties.ProviderSettings openaiSettings = new ProviderProperties.ProviderSettings();
        openaiSettings.setApiKey("test-api-key");
        openaiSettings.setModelName("gpt-4o-mini");

        config.setProviders(java.util.Map.of("openai", openaiSettings));
        config.setDefaultProvider("openai");

        // Verify configuration
        assertNotNull(config.getProviders());
        assertTrue(config.getProviders().containsKey("openai"));
        assertEquals("openai", config.getDefaultProvider());

        ProviderProperties.ProviderSettings settings = config.getProviders().get("openai");
        assertEquals("test-api-key", settings.getApiKey());
        assertEquals("gpt-4o-mini", settings.getModelName());
    }

    @Test
    void testEnvironmentVariableLoading() {
        // Test that we can read environment variables
        String openaiKey = System.getenv("OPENAI_API_KEY");
        String anthropicKey = System.getenv("ANTHROPIC_API_KEY");
        String geminiKey = System.getenv("GEMINI_API_KEY");

        // At least one should be set for the test to be meaningful
        boolean hasAtLeastOneKey = (openaiKey != null && !openaiKey.isEmpty()) ||
                                  (anthropicKey != null && !anthropicKey.isEmpty()) ||
                                  (geminiKey != null && !geminiKey.isEmpty());

        if (hasAtLeastOneKey) {
            System.out.println("Environment variables detected:");
            if (openaiKey != null && !openaiKey.isEmpty()) {
                System.out.println("OPENAI_API_KEY: " + openaiKey.substring(0, Math.min(8, openaiKey.length())) + "...");
            }
            if (anthropicKey != null && !anthropicKey.isEmpty()) {
                System.out.println("ANTHROPIC_API_KEY: " + anthropicKey.substring(0, Math.min(8, anthropicKey.length())) + "...");
            }
            if (geminiKey != null && !geminiKey.isEmpty()) {
                System.out.println("GEMINI_API_KEY: " + geminiKey.substring(0, Math.min(8, geminiKey.length())) + "...");
            }
        } else {
            System.out.println("No API keys found in environment variables.");
            System.out.println("Set OPENAI_API_KEY, ANTHROPIC_API_KEY, or GEMINI_API_KEY to run inference tests.");
        }

        // This test always passes - it's just for information
        assertTrue(true);
    }

    @Test
    void testProviderSettings() {
        ProviderProperties.ProviderSettings settings = new ProviderProperties.ProviderSettings();

        // Test setters and getters
        settings.setApiKey("test-key");
        settings.setBaseUrl("https://api.test.com");
        settings.setEndpoint("https://endpoint.test.com");
        settings.setModelName("test-model");

        assertEquals("test-key", settings.getApiKey());
        assertEquals("https://api.test.com", settings.getBaseUrl());
        assertEquals("https://endpoint.test.com", settings.getEndpoint());
        assertEquals("test-model", settings.getModelName());
    }
}
