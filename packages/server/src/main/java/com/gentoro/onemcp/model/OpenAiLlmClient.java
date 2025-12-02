package com.gentoro.onemcp.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.openai.client.OpenAIClient;
import com.openai.core.JsonValue;
import com.openai.models.ChatModel;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

/** OpenAI implementation of {@link LlmClient} using openai-java SDK (Chat Completions API). */
public class OpenAiLlmClient extends AbstractLlmClient {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(OpenAiLlmClient.class);
  private final OpenAIClient openAIClient;

  public OpenAiLlmClient(OneMcp oneMcp, OpenAIClient openAIClient, Configuration configuration) {
    super(oneMcp, configuration);
    this.openAIClient = openAIClient;
  }

  private ChatCompletionCreateParams.Builder initialize(List<Message> messages, List<Tool> tools) {
    String modelId = configuration.getString("model", ChatModel.GPT_4_1.toString());
    ChatModel model;
    try {
      model = ChatModel.of(modelId);
    } catch (Exception e) {
      // fallback to GPT-4.1 if unknown
      model = ChatModel.GPT_4_1;
    }

    ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder().model(model);

    if (Message.contains(messages, Role.SYSTEM)) {
      builder.addSystemMessage(Message.findFirst(messages, Role.SYSTEM).content());
    }

    if (tools != null && !tools.isEmpty()) {
      builder.tools(tools.stream().map(t -> convertTool(t.definition())).toList());
    }

    Message.allExcept(messages, Role.SYSTEM)
        .forEach(
            message -> {
              if (message.role() == Role.ASSISTANT) {
                builder.addAssistantMessage(message.content());
              } else if (message.role() == Role.USER) {
                builder.addUserMessage(message.content());
              } else {
                throw new com.gentoro.onemcp.exception.StateException(
                    "Unknown message role: " + message.role());
              }
            });
    return builder;
  }

  @Override
  public String runContentGeneration(
      String message, List<Tool> tools, InferenceEventListener listener) {
    ChatCompletionCreateParams.Builder builder = initialize(Collections.emptyList(), tools);

    long start = System.currentTimeMillis();
    String modelId = configuration.getString("model", ChatModel.GPT_4_1.toString());
    TelemetrySink t =
        setupTelemetry("openai", modelId, tools == null ? 0 : tools.size(), 0, "generate");

    ChatCompletion chatCompletion = openAIClient.chat().completions().create(builder.build());
    if (listener != null) listener.on(EventType.ON_COMPLETION, chatCompletion);

    if (chatCompletion.choices().isEmpty()) {
      if (t != null) {
        long end = System.currentTimeMillis();
        t.endCurrentError(java.util.Map.of("latencyMs", (end - start), "error", "No choices"));
      }
      throw new com.gentoro.onemcp.exception.LlmException(
          "No candidates returned from OpenAI inference.");
    }

    String content =
        chatCompletion.choices().stream()
            .filter(c -> c.message().content().isPresent())
            .map(c -> c.message().content().get())
            .findFirst()
            .orElseThrow(
                () ->
                    new com.gentoro.onemcp.exception.LlmException(
                        "No content returned from Gemini inference."));
    long end = System.currentTimeMillis();
    long promptTokens = 0;
    long completionTokens = 0;
    Long totalTokens = null;
    if (chatCompletion.usage().isPresent()) {
      promptTokens = chatCompletion.usage().get().promptTokens();
      completionTokens = chatCompletion.usage().get().completionTokens();
      totalTokens = Long.valueOf(chatCompletion.usage().get().totalTokens());
    }

    finishTelemetry(t, (end - start), promptTokens, completionTokens, totalTokens, content);
    return content;
  }

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {
    ChatCompletionCreateParams.Builder builder = initialize(messages, tools);
    final TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};

    String result = null;
    main_loop:
    while (true) {
      long start = System.currentTimeMillis();
      TelemetrySink t2 = telemetry();
      if (t2 != null) {
        t2.startChild("llm.openai");
        t2.currentAttributes().put("provider", "openai");
        String modelId = configuration.getString("model", ChatModel.GPT_4_1.toString());
        t2.currentAttributes().put("model", modelId);
        t2.currentAttributes().put("tools.count", tools == null ? 0 : tools.size());
        t2.currentAttributes().put("messages.count", messages == null ? 0 : messages.size());
        t2.currentAttributes().put("mode", "chat");
      }
      ChatCompletion chatCompletion = openAIClient.chat().completions().create(builder.build());
      if (listener != null) listener.on(EventType.ON_COMPLETION, chatCompletion);

      final List<ChatCompletionMessageToolCall> llmToolCalls = new ArrayList<>();
      ChatCompletion.Choice choice = chatCompletion.choices().getFirst();

      long end = System.currentTimeMillis();
      long promptTokens =
          chatCompletion.usage().isPresent() ? chatCompletion.usage().get().promptTokens() : 0;
      long completionTokens =
          chatCompletion.usage().isPresent() ? chatCompletion.usage().get().completionTokens() : 0;
      String responseText = choice.message().content().map(String::trim).orElse("");

      log.info(
          "[Inference] - OpenAI:\nLLM inference took {} ms.\nTotal tokens {}.\n---\n",
          (end - start),
          chatCompletion.usage().get().totalTokens());
      if (t2 != null) {
        chatCompletion
            .usage()
            .ifPresent(
                u ->
                    t2.addUsage(
                        Long.valueOf(u.promptTokens()),
                        Long.valueOf(u.completionTokens()),
                        Long.valueOf(u.totalTokens())));
        Long total =
            chatCompletion.usage().isPresent()
                ? Long.valueOf(chatCompletion.usage().get().totalTokens())
                : null;
        t2.endCurrentOk(
            java.util.Map.of(
                "latencyMs", (end - start), "usage.total", total, "response", responseText));
      }

      builder.addMessage(choice.message());
      if (choice.message().toolCalls().isPresent()) {
        llmToolCalls.addAll(choice.message().toolCalls().get());
      }

      if (llmToolCalls.isEmpty()) {
        result = choice.message().content().map(String::trim).orElse("");
        break;
      } else {
        // Execute tools and feed responses back to the model
        for (ChatCompletionMessageToolCall toolCall : llmToolCalls) {
          try {
            Tool tool =
                tools.stream()
                    .filter(t -> t.name().equals(toolCall.function().get().function().name()))
                    .findFirst()
                    .orElseThrow();
            listener.on(EventType.ON_TOOL_CALL, tool);
            HashMap<String, Object> values =
                JacksonUtility.getJsonMapper()
                    .readValue(
                        Objects.requireNonNullElse(
                            toolCall.function().get().function().arguments(), "{}"),
                        typeRef);

            String content = tool.execute(values);
            builder.addMessage(
                ChatCompletionToolMessageParam.builder()
                    .toolCallId(toolCall.function().get().id())
                    .content(content)
                    .build());
          } catch (Exception e) {
            throw new com.gentoro.onemcp.exception.LlmException(
                "Failed to execute tool: " + toolCall.function().get().function().name(), e);
          }
        }
      }
    }
    listener.on(EventType.ON_END, builder.build().messages());
    return result;
  }

  private Map<String, Object> convertProperty(ToolProperty property) {
    Map<String, Object> result = new HashMap<>();
    result.put("type", asOpenAiType(property.getType()));
    result.put("description", property.getDescription());
    if (property.getType() == ToolProperty.Type.ARRAY) {
      result.put("items", convertProperty(property.getItems()));
      result.put("additionalProperties", false);
    } else if (property.getType() == ToolProperty.Type.OBJECT) {
      result.put(
          "properties",
          property.getProperties().stream()
              .collect(Collectors.toMap(ToolProperty::getName, this::convertProperty)));
      result.put(
          "required",
          property.getProperties().stream()
              .filter(ToolProperty::isRequired)
              .map(ToolProperty::getName)
              .toList());
      result.put("additionalProperties", false);
    }

    return result;
  }

  private FunctionParameters convertSchema(ToolProperty property) {
    FunctionParameters.Builder paramsBuilder = FunctionParameters.builder();
    convertProperty(property)
        .forEach((key, value) -> paramsBuilder.putAdditionalProperty(key, JsonValue.from(value)));
    return paramsBuilder.build();
  }

  private String asOpenAiType(ToolProperty.Type type) {
    return switch (type) {
      case OBJECT -> ("object");
      case NUMBER -> ("number");
      case ARRAY -> ("array");
      case BOOLEAN -> ("boolean");
      default -> ("string");
    };
  }

  private ChatCompletionTool convertTool(ToolDefinition def) {
    FunctionDefinition.Builder functionBuilder =
        FunctionDefinition.builder()
            .name(def.name())
            .description(def.description())
            .parameters(convertSchema(def.schema()));

    functionBuilder.parameters(convertSchema(def.schema()));
    return ChatCompletionTool.ofFunction(
            ChatCompletionFunctionTool.builder().function(functionBuilder.build()).build())
        .validate();
  }
}
