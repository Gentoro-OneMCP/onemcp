package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import io.github.ollama4j.Ollama;
import io.github.ollama4j.exceptions.OllamaException;
import io.github.ollama4j.models.chat.OllamaChatMessageRole;
import io.github.ollama4j.models.chat.OllamaChatRequest;
import io.github.ollama4j.models.chat.OllamaChatResult;
import io.github.ollama4j.models.chat.OllamaChatStreamObserver;
import io.github.ollama4j.models.generate.OllamaGenerateTokenHandler;
import io.github.ollama4j.tools.Tools;
import io.github.ollama4j.utils.Options;
import io.github.ollama4j.utils.OptionsBuilder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.Configuration;

/** OpenAI implementation of LlmClient using openai-java SDK (Chat Completions API). */
public class OllamaLlmClient extends AbstractLlmClient {
  public OllamaLlmClient(OneMcp oneMcp, Configuration configuration) {
    super(oneMcp, configuration);
  }

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {
    final Ollama ollama = new Ollama(configuration.getString("baseUrl", "http://localhost:11434"));
    ollama.setRequestTimeoutSeconds(TimeUnit.MINUTES.toMillis(5));
    if (tools != null && !tools.isEmpty()) {
      tools.forEach(t -> ollama.registerTool(convertTool(t, listener)));
    }

    Options options = new OptionsBuilder().setTemperature(0.3f).setRepeatPenalty(1.5f).build();

    OllamaChatRequest builder =
        OllamaChatRequest.builder()
            .withModel(configuration.getString("model"))
            .withUseTools(true)
            .withOptions(options)
            .withTools(new ArrayList<>());
    // .withTools(tools == null ? Collections.emptyList() : tools.stream().map(t -> convertTool(t,
    // listener)).toList());
    List<Message> localMessages = new ArrayList<>();
    localMessages.addAll(messages);

    localMessages.forEach(
        m ->
            builder.withMessage(
                switch (m.role()) {
                  case USER -> OllamaChatMessageRole.USER;
                  case ASSISTANT -> OllamaChatMessageRole.ASSISTANT;
                  case SYSTEM -> OllamaChatMessageRole.SYSTEM;
                  default -> throw new com.gentoro.onemcp.exception.StateException(
                      "Unknown message role: " + m.role());
                },
                m.content()));

    try {
      OllamaChatResult chatCompletion;
      do {
        long start = System.currentTimeMillis();

        // Define a stream observer.
        OllamaChatStreamObserver streamObserver = new OllamaChatStreamObserver();
        System.out.println("Starting LLM inference...");
        // If thinking tokens are found, print them in lowercase :)
        streamObserver.setThinkingStreamHandler(
            new OllamaGenerateTokenHandler() {
              @Override
              public void accept(String message) {
                if (message.contains("\n") || message.contains("\r")) {
                  System.out.print("\rThinking: " + message.replaceAll("\\r?\\n", " "));
                  System.out.flush();
                } else {
                  System.out.print(message);
                }
              }
            });
        // Response tokens to be printed in lowercase
        streamObserver.setResponseStreamHandler(
            new OllamaGenerateTokenHandler() {
              @Override
              public void accept(String message) {
                if (message.contains("\n") || message.contains("\r")) {
                  System.out.printf(
                      "\rGenerating (OrchestratorApp: %s): %s",
                      configuration.getString("model"), message.replaceAll("\\r?\\n", " "));
                  System.out.flush();
                } else {
                  System.out.print(message);
                }
              }
            });

        chatCompletion = ollama.chat(builder.build(), streamObserver);
        listener.on(EventType.ON_COMPLETION, chatCompletion);
        long end = System.currentTimeMillis();
        System.out.flush();
        System.out.printf(
            "[Inference] - OrchestratorApp(%s):\n"
                + "LLM inference took %d ms.\n"
                + "Total tokens %s.\n"
                + "---\n%n",
            configuration.getString("model"),
            (end - start),
            chatCompletion.getResponseModel().getPromptEvalCount());

        listener.on(EventType.ON_END, chatCompletion.getResponseModel().getMessage().getResponse());
      } while (!chatCompletion.getResponseModel().isDone());
      return chatCompletion.getResponseModel().getMessage().getResponse();
    } catch (OllamaException e) {
      throw new com.gentoro.onemcp.exception.LlmException("Failed to run LLM inference", e);
    }
  }

  private String asOllamaType(ToolProperty.Type type) {
    return switch (type) {
      case OBJECT -> ("object");
      case NUMBER -> ("number");
      case ARRAY -> ("array");
      case BOOLEAN -> ("boolean");
      default -> ("string");
    };
  }

  private Tools.Tool convertTool(final Tool tool, final InferenceEventListener listener) {
    ToolDefinition def = tool.definition();
    Map<String, Tools.Property> properties = new HashMap<>();
    if (def.schema() != null) {
      tool.definition().schema().getProperties().stream()
          .forEach(
              param -> {
                Tools.Property.PropertyBuilder builder =
                    Tools.Property.builder()
                        .type(asOllamaType(param.getType()))
                        .description(param.getDescription())
                        .required(false);
                properties.put(param.getName(), builder.build());
              });
    }

    new Tools.Parameters();

    return Tools.Tool.builder()
        .toolSpec(
            Tools.ToolSpec.builder()
                .name(def.name())
                .description(def.description())
                .parameters(Tools.Parameters.of(properties))
                .build())
        .toolFunction(
            arguments -> {
              listener.on(EventType.ON_TOOL_CALL, tool);
              return tool.execute(arguments);
            })
        .build();
  }
}
