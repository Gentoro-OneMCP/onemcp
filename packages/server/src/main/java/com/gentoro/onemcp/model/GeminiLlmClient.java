package com.gentoro.onemcp.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.messages.ExecutionPlan;
import com.gentoro.onemcp.messages.StepImplementation;
import com.gentoro.onemcp.messages.Summary;
import com.google.genai.Client;
import com.google.genai.types.FinishReason;
import com.google.genai.types.FunctionCallingConfigMode;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.Type;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

/**
 * Minimal Gemini implementation placeholder. For now it does not support tool-calling. It can be
 * extended to integrate with Google's Generative AI Java SDK.
 */
public class GeminiLlmClient extends AbstractLlmClient {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(GeminiLlmClient.class);
  private final Client geminiClient;

  public GeminiLlmClient(OneMcp oneMcp, Client geminiClient, Configuration configuration) {
    super(oneMcp, configuration);
    this.geminiClient = geminiClient;
  }

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {

    GenerateContentConfig.Builder configBuilder =
        GenerateContentConfig.builder()
            .temperature(configuration.getFloat("options.temperature", 0.7f))
            .candidateCount(configuration.getInt("options.candidate-count", 1));

    if (!tools.isEmpty()) {
      List<com.google.genai.types.FunctionDeclaration> functionDeclarations = new ArrayList<>();
      tools.stream().map(tool -> convertTool(tool.definition())).forEach(functionDeclarations::add);
      configBuilder
          .tools(
              com.google.genai.types.Tool.builder()
                  .functionDeclarations(functionDeclarations)
                  .build())
          .toolConfig(
              com.google.genai.types.ToolConfig.builder()
                  .functionCallingConfig(
                      com.google.genai.types.FunctionCallingConfig.builder()
                          .mode(FunctionCallingConfigMode.Known.ANY)
                          .build())
                  .build());
    }

    if (Message.contains(messages, Role.SYSTEM)) {
      configBuilder.systemInstruction(
          com.google.genai.types.Content.builder()
              .role("user")
              .parts(
                  com.google.genai.types.Part.fromText(
                      Message.findFirst(messages, Role.SYSTEM).content()))
              .build());
    }

    List<com.google.genai.types.Content> localMessages = new ArrayList<>();
    Message.allExcept(messages, Role.SYSTEM)
        .forEach(message -> localMessages.add(asGeminiMessage(message)));

    int attempts = 0;
    com.google.genai.types.GenerateContentResponse chatCompletions;
    main_loop:
    while (true) {
      long start = System.currentTimeMillis();
      String modelName = configuration.getString("model", "gemini-2.5-flash");
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
      for (com.google.genai.types.Candidate candidate : chatCompletions.candidates().get()) {
        if (candidate.content().isEmpty()) {
          continue;
        }

        com.google.genai.types.Content content = candidate.content().get();
        if (content.parts().isEmpty()) {
          continue;
        }

        localMessages.add(content);

        for (com.google.genai.types.Part part : content.parts().get()) {
          if (part.functionCall().isEmpty() || part.functionCall().get().name().isEmpty()) {
            continue;
          }

          com.google.genai.types.FunctionCall functionCall = part.functionCall().get();
          Tool tool =
              tools.stream()
                  .filter(t -> t.name().equals(functionCall.name().get()))
                  .findFirst()
                  .orElse(null);

          if (tool == null) {
            com.google.genai.types.FunctionResponse.Builder functionResponse =
                com.google.genai.types.FunctionResponse.builder().name(functionCall.name().get());
            functionCall.id().ifPresent(functionResponse::id);
            functionResponse.response(
                Map.of(
                    "result",
                    "There are no Tool / Function name `"
                        + functionCall.name().get()
                        + "`. Refer to one of the supported functions: "
                        + tools.stream().map(Tool::name).collect(Collectors.joining(", "))));

            localMessages.add(
                com.google.genai.types.Content.builder()
                    .role("user")
                    .parts(
                        com.google.genai.types.Part.builder()
                            .functionResponse(functionResponse.build())
                            .build())
                    .build());
            continue;
          }

          listener.on(EventType.ON_TOOL_CALL, tool);
          try {
            Map<String, Object> values = functionCall.args().get();
            String result = tool.execute(values);

            com.google.genai.types.FunctionResponse.Builder functionResponse =
                com.google.genai.types.FunctionResponse.builder().name(functionCall.name().get());
            functionCall.id().ifPresent(functionResponse::id);
            functionResponse.response(Map.of("result", result));

            localMessages.add(
                com.google.genai.types.Content.builder()
                    .role("user")
                    .parts(
                        com.google.genai.types.Part.builder()
                            .functionResponse(functionResponse.build())
                            .build())
                    .build());

            if (tool.name().equals(ExecutionPlan.class.getSimpleName())
                || tool.name().equals(StepImplementation.class.getSimpleName())
                || tool.name().equals(Summary.class.getSimpleName())) {
              break main_loop;
            }

          } catch (Exception toolExecError) {
            com.google.genai.types.FunctionResponse.Builder functionResponse =
                com.google.genai.types.FunctionResponse.builder().name(functionCall.name().get());
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
                com.google.genai.types.Content.builder()
                    .role("user")
                    .parts(
                        com.google.genai.types.Part.builder()
                            .functionResponse(functionResponse.build())
                            .build())
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

  private com.google.genai.types.Content asGeminiMessage(Message message) {
    return com.google.genai.types.Content.builder()
        .role(
            switch (message.role()) {
              case USER -> "user";
              case ASSISTANT -> "model";
              default -> throw new com.gentoro.onemcp.exception.StateException(
                  "Unknown message role: " + message.role());
            })
        .parts(com.google.genai.types.Part.fromText(message.content()))
        .build();
  }

  private com.google.genai.types.FunctionDeclaration convertTool(ToolDefinition def) {
    return com.google.genai.types.FunctionDeclaration.builder()
        .name(def.name())
        .description(def.description())
        .parameters(def.schema() != null ? propSchema(def.schema()) : null)
        .build();
  }

  private com.google.genai.types.Schema propSchema(ToolProperty property) {
    com.google.genai.types.Schema.Builder propSchemaBuilder =
        com.google.genai.types.Schema.builder()
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
