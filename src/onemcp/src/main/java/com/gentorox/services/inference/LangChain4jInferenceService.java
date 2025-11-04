package com.gentorox.services.inference;

import com.gentorox.services.telemetry.TelemetryService;
import com.gentorox.services.telemetry.TelemetrySession;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.googleai.GeminiMode;
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.MemoryId;
import dev.langchain4j.service.UserMessage;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Example of how to use LangChain4j's native tool system with @Tool annotations.
 * This approach uses AiServices to automatically handle tool calling.
 */
@Service
public class LangChain4jInferenceService {

    private final ChatLanguageModel chatModel;
    private final TelemetryService telemetry;
    private final String provider;
    private final String modelName;

    // Define the AI service interface with tools
    public interface AiAssistant {
        @dev.langchain4j.service.UserMessage("{{message}}")
        String chat(@MemoryId String sessionId, String message);
    }

    public LangChain4jInferenceService(ProviderProperties providerProperties, TelemetryService telemetry) {
        this.chatModel = createChatModel(providerProperties);
        this.telemetry = telemetry;
        this.provider = providerProperties.getDefaultProvider();
        this.modelName = providerProperties.getProviders().get(provider).getModelName();
    }

    /**
     * Sends an inference request using LangChain4j's native tool system.
     * Tools are automatically discovered and executed by the AI service.
     */
    public String sendRequestWithTools(String prompt, Object... toolInstances) {
        TelemetrySession session = TelemetrySession.create();

        return telemetry.runRoot(session, "langchain4j.inference.request", Map.of(
            "gentorox.inference.provider", provider,
            "gentorox.inference.model", modelName,
            "gentorox.inference.tools.count", String.valueOf(toolInstances.length)
        ), () -> {
            // Count the prompt
            telemetry.countPrompt(provider, modelName);

            // Create AI service with tools
            AiAssistant assistant = AiServices.builder(AiAssistant.class)
                .chatLanguageModel(chatModel)
                .chatMemory(MessageWindowChatMemory.withMaxMessages(10))
                .tools(toolInstances) // Pass tool instances directly
                .build();

            // Execute the request - tools are automatically called as needed
            return assistant.chat(session.id(), prompt);
        });
    }

    private ChatLanguageModel createChatModel(ProviderProperties providerProperties) {
        String provider = providerProperties.getDefaultProvider();
        ProviderProperties.ProviderSettings settings = providerProperties.getProviders().get(provider);

        if (settings == null) {
            throw new IllegalArgumentException("Provider configuration not found for: " + provider);
        }

        return switch (provider.toLowerCase()) {
            case "openai" -> {
                if (settings.getApiKey() == null || settings.getApiKey().isEmpty()) {
                    throw new IllegalArgumentException("OpenAI API key is required");
                }
                var builder = OpenAiChatModel.builder()
                    .apiKey(settings.getApiKey())
                    .modelName(settings.getModelName());

                if (settings.getBaseUrl() != null && !settings.getBaseUrl().isEmpty()) {
                    builder.baseUrl(settings.getBaseUrl());
                }

                yield builder.build();
            }
          case "gemini" -> {
            if (settings.getApiKey() == null || settings.getApiKey().isEmpty()) {
              throw new IllegalArgumentException("Gemini API key is required");
            }
            var builder = GoogleAiGeminiChatModel.builder()
                .apiKey(settings.getApiKey())
                .temperature(0.3D)
                .toolConfig(GeminiMode.ANY)
                .modelName(settings.getModelName());

            yield builder.build();
          }
            default -> throw new IllegalArgumentException("Unsupported model provider: " + provider);
        };
    }
}
