package com.gentoro.onemcp.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.gentoro.onemcp.OneMcp;
import com.google.genai.Client;
import com.google.genai.types.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

public class GeminiLlmClient extends AbstractLlmClient {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(GeminiLlmClient.class);
  private final Client geminiClient;

  public GeminiLlmClient(OneMcp oneMcp, Client geminiClient, Configuration configuration) {
    super(oneMcp, configuration);
    this.geminiClient = geminiClient;
  }

  private GenerateContentConfig.Builder initializeConfigBuilder(List<Tool> tools) {
    GenerateContentConfig.Builder configBuilder =
        GenerateContentConfig.builder()
            .temperature(configuration.getFloat("options.temperature", 0.7f))
            .candidateCount(configuration.getInt("options.candidate-count", 1));

    if (!tools.isEmpty()) {
      List<FunctionDeclaration> functionDeclarations = new ArrayList<>();
      tools.stream().map(tool -> convertTool(tool.definition())).forEach(functionDeclarations::add);
      configBuilder
          .tools(
              com.google.genai.types.Tool.builder()
                  .functionDeclarations(functionDeclarations)
                  .build())
          .toolConfig(
              ToolConfig.builder()
                  .functionCallingConfig(
                      FunctionCallingConfig.builder()
                          .mode(FunctionCallingConfigMode.Known.ANY)
                          .build())
                  .build());
    }
    return configBuilder;
  }

  @Override
  public String runContentGeneration(
      String message, List<Tool> tools, InferenceEventListener listener) {
    GenerateContentConfig.Builder configBuilder = initializeConfigBuilder(tools);

    String modelName = configuration.getString("model", "gemini-2.5-flash");
    GenerateContentResponse chatCompletion =
        geminiClient.models.generateContent(modelName, message, configBuilder.build());
    List<Candidate> candidates =
        chatCompletion
            .candidates()
            .orElseThrow(
                () ->
                    new com.gentoro.onemcp.exception.LlmException(
                        "No candidates returned from Gemini inference."));
    if (candidates.isEmpty()) {
      throw new com.gentoro.onemcp.exception.LlmException(
          "No candidates returned from Gemini inference.");
    }

    Content content =
        candidates.stream()
            .filter(c -> c.content().isPresent())
            .map(c -> c.content().get())
            .findFirst()
            .orElseThrow(
                () ->
                    new com.gentoro.onemcp.exception.LlmException(
                        "No content returned from Gemini inference."));

    return content.text();
  }

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {

    GenerateContentConfig.Builder configBuilder = initializeConfigBuilder(tools);
    String modelName = configuration.getString("model", "gemini-2.5-flash");

    if (messages.isEmpty() || Message.allExcept(messages, Role.SYSTEM).isEmpty()) {
      throw new com.gentoro.onemcp.exception.LlmException("No messages provided to run inference.");
    }

    if (Message.contains(messages, Role.SYSTEM)) {
      configBuilder.systemInstruction(
          Content.builder()
              .role("user")
              .parts(Part.fromText(Message.findFirst(messages, Role.SYSTEM).content()))
              .build());
    }

    List<Content> localMessages = new ArrayList<>();
    Message.allExcept(messages, Role.SYSTEM)
        .forEach(message -> localMessages.add(asGeminiMessage(message)));

    int attempts = 0;
    GenerateContentResponse chatCompletions;
    main_loop:
    while (true) {
      long start = System.currentTimeMillis();

      log.trace("Running inference with model: {}", modelName);
      chatCompletions =
          geminiClient.models.generateContent(modelName, localMessages, configBuilder.build());
      listener.on(EventType.ON_COMPLETION, chatCompletions);

      long end = System.currentTimeMillis();
      log.trace(
          "Gemini inference took {} ms, and a total of {} token(s).",
          (end - start),
          chatCompletions.usageMetadata().get().totalTokenCount().get());

      if (chatCompletions.finishReason().knownEnum()
          == FinishReason.Known.MALFORMED_FUNCTION_CALL) {
        if (++attempts == 3) {
          throw new com.gentoro.onemcp.exception.LlmException(
              "Gemini consistently failed with MALFORMED_FUNCTION_CALL after %d attempts; aborting inference."
                  .formatted(attempts));
        } else {
          chatCompletions
              .candidates()
              .flatMap(candidate -> candidate.getFirst().content())
              .ifPresent(localMessages::add);

          localMessages.add(
              asGeminiMessage(
                  new Message(
                      Role.USER,
                      "Seems you are having trouble calling the function. "
                          + "Spend a bit more time on all provide information and elaborate the proper function call with the valid set of parameters.")));

          log.warn("Gemini stopped due to MALFORMED_FUNCTION_CALL, trying once more.\n---\n");
          continue main_loop;
        }
      }

      if (chatCompletions.candidates().isEmpty() || chatCompletions.candidates().get().isEmpty()) {
        break;
      }
      TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {};
      for (Candidate candidate : chatCompletions.candidates().get()) {
        if (candidate.content().isEmpty()) {
          continue;
        }

        Content content = candidate.content().get();
        if (content.parts().isEmpty()) {
          continue;
        }

        localMessages.add(content);

        for (Part part : content.parts().get()) {
          if (part.functionCall().isEmpty() || part.functionCall().get().name().isEmpty()) {
            continue;
          }

          FunctionCall functionCall = part.functionCall().get();
          Tool tool =
              tools.stream()
                  .filter(t -> t.name().equals(functionCall.name().get()))
                  .findFirst()
                  .orElse(null);

          if (tool == null) {
            FunctionResponse.Builder functionResponse =
                FunctionResponse.builder().name(functionCall.name().get());
            functionCall.id().ifPresent(functionResponse::id);
            functionResponse.response(
                Map.of(
                    "result",
                    "There are no Tool / Function name `"
                        + functionCall.name().get()
                        + "`. Refer to one of the supported functions: "
                        + tools.stream().map(Tool::name).collect(Collectors.joining(", "))));

            localMessages.add(
                Content.builder()
                    .role("user")
                    .parts(Part.builder().functionResponse(functionResponse.build()).build())
                    .build());
            continue;
          }

          listener.on(EventType.ON_TOOL_CALL, tool);
          try {
            Map<String, Object> values = functionCall.args().get();
            String result = tool.execute(values);

            FunctionResponse.Builder functionResponse =
                FunctionResponse.builder().name(functionCall.name().get());
            functionCall.id().ifPresent(functionResponse::id);
            functionResponse.response(Map.of("result", result));

            localMessages.add(
                Content.builder()
                    .role("user")
                    .parts(Part.builder().functionResponse(functionResponse.build()).build())
                    .build());
          } catch (Exception toolExecError) {
            FunctionResponse.Builder functionResponse =
                FunctionResponse.builder().name(functionCall.name().get());
            functionCall.id().ifPresent(functionResponse::id);
            functionResponse.response(
                Map.of(
                    "isError",
                    true,
                    "errorMessage",
                    "Error executing function call: "
                        + functionCall.name()
                        + ", understand the error and report back with the most appropriate context.",
                    "errorDetails",
                    toolExecError.getMessage()));
            localMessages.add(
                Content.builder()
                    .role("user")
                    .parts(Part.builder().functionResponse(functionResponse.build()).build())
                    .build());
          }
        }

        break;
      }

      if (chatCompletions.finishReason() != null
          && (chatCompletions.functionCalls() == null
              || chatCompletions.functionCalls().isEmpty())) {
        // Process complete, nothing else to do.
        break;
      }
    }
    listener.on(EventType.ON_END, localMessages);
    return localMessages.getLast().text();
  }

  private Content asGeminiMessage(Message message) {
    return Content.builder()
        .role(
            switch (message.role()) {
              case USER -> "user";
              case ASSISTANT -> "model";
              default -> throw new com.gentoro.onemcp.exception.StateException(
                  "Unknown message role: " + message.role());
            })
        .parts(Part.fromText(message.content()))
        .build();
  }

  private FunctionDeclaration convertTool(ToolDefinition def) {
    return FunctionDeclaration.builder()
        .name(def.name())
        .description(def.description())
        .parameters(def.schema() != null ? propSchema(def.schema()) : null)
        .build();
  }

  private Schema propSchema(ToolProperty property) {
    Schema.Builder propSchemaBuilder =
        Schema.builder()
            .type(asGeminiType(property.getType()))
            .description(property.getDescription());
    if (property.getName() != null) {
      propSchemaBuilder.title(property.getName());
    }

    if (property.getType() == ToolProperty.Type.ARRAY) {
      propSchemaBuilder.items(propSchema(property.getItems()));
    } else if (property.getType() == ToolProperty.Type.OBJECT) {
      propSchemaBuilder.properties(
          property.getProperties().stream()
              .map(p -> Map.entry(p.getName(), propSchema(p)))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      propSchemaBuilder.required(
          property.getProperties().stream()
              .filter(ToolProperty::isRequired)
              .map(ToolProperty::getName)
              .toList());
    }
    return propSchemaBuilder.build();
  }

  private Type.Known asGeminiType(ToolProperty.Type type) {
    return switch (type) {
      case OBJECT -> Type.Known.OBJECT;
      case NUMBER -> Type.Known.NUMBER;
      case ARRAY -> Type.Known.ARRAY;
      case BOOLEAN -> Type.Known.BOOLEAN;
      default -> Type.Known.STRING;
    };
  }
}
