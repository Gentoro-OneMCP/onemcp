package com.gentorox.services.inference;

import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.telemetry.TelemetryService;
import com.gentorox.services.telemetry.TelemetrySession;
import com.gentorox.tools.AgentTool;
import dev.langchain4j.memory.chat.ChatMemoryProvider;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
// Temporarily disabled due to missing API key
// import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.MemoryId;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.store.memory.chat.ChatMemoryStore;
import dev.langchain4j.store.memory.chat.InMemoryChatMemoryStore;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * InferenceService that provides a unified interface for sending inference requests
 * to various AI models using LangChain4j's native tool system with @Tool annotations.
 */
public class InferenceService {

  private final ChatLanguageModel chatModel;
  private final TelemetryService telemetry;
  private final String provider;
  private final String modelName;
  private final ApplicationContext applicationContext;
  // shared store so memories persist per sessionId
  private final ChatMemoryStore store = new InMemoryChatMemoryStore();

  // Define the AI service interface
  public interface AiAssistant {
    String chat(@MemoryId String sessionId, @UserMessage String message);
  }

  public InferenceService(ProviderProperties providerProperties, TelemetryService telemetry) {
    this(null, providerProperties, telemetry);
  }

  public InferenceService(ApplicationContext applicationContext, ProviderProperties providerProperties, TelemetryService telemetry) {
    this.applicationContext = applicationContext;
    this.chatModel = createChatModel(providerProperties);
    this.telemetry = telemetry;
    this.provider = providerProperties.getDefaultProvider();
    this.modelName = providerProperties.getProviders().get(provider).getModelName();
  }

  /**
   * Sends an inference request to the currently configured model with LangChain4j tools.
   * Automatically executes any tools requested by the AI model and returns the final response.
   *
   * @param prompt The input string/prompt to send to the model
   * @param toolInstances Array of tool instances (objects with @Tool methods)
   * @return InferenceResponse containing the model's final response after tool execution
   */
  public InferenceResponse sendRequest(String prompt, Object... toolInstances) {
    // Create telemetry session for this request
    TelemetrySession session = TelemetrySession.create();

    // Wrap the entire inference request in a root span
    return telemetry.inSpan("inference.request", Map.of(
        "gentorox.inference.provider", provider,
        "gentorox.inference.model", modelName,
        "gentorox.inference.tools.count", String.valueOf(toolInstances.length),
        "gentorox.inference.prompt.length", String.valueOf(prompt.length())
    ), () -> {
      try {
        // Count the prompt
        telemetry.countPrompt(provider, modelName);
        // provider builds/fetches memory for each sessionId
        ChatMemoryProvider provider = sessionId ->
            MessageWindowChatMemory.builder().chatMemoryStore(store).maxMessages(10).id(sessionId).build();

        // Create AI service with tools
        List<Object> listOfAvailableTools = applicationContext != null ? new ArrayList<>(applicationContext.getBeansOfType(AgentTool.class).values()) : new ArrayList<>();
        if( toolInstances != null ) {
          for( Object o : toolInstances ) {
            if( o != null ) listOfAvailableTools.add(o);
          }
        }
        AiAssistant assistant = AiServices.builder(AiAssistant.class)
            .chatLanguageModel(chatModel)
            .chatMemoryProvider(provider)
            .tools(listOfAvailableTools) // Pass tool instances directly
            .build();

        // Execute the request - tools are automatically called as needed
        String response = assistant.chat(session.id(), prompt);

        return new InferenceResponse(response, java.util.Optional.empty(), "langchain4j_response");

      } catch (Exception e) {
        throw new RuntimeException("Failed to send inference request", e);
      }
    });
  }
  /**
   * Creates a ChatLanguageModel based on the configured provider.
   * Logs which provider/model is being instantiated (without exposing secrets).
   */
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
            .temperature(1D)
            .modelName(settings.getModelName());

        if (settings.getBaseUrl() != null && !settings.getBaseUrl().isEmpty()) {
          builder.baseUrl(settings.getBaseUrl());
        }

        yield builder.build();
      }
      case "anthropic" -> {
        if (settings.getApiKey() == null || settings.getApiKey().isEmpty()) {
          throw new IllegalArgumentException("Anthropic API key is required");
        }
        var builder = AnthropicChatModel.builder()
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
            .modelName(settings.getModelName());

        yield builder.build();
      }
      default -> throw new IllegalArgumentException("Unsupported model provider: " + provider);
    };
  }
}
