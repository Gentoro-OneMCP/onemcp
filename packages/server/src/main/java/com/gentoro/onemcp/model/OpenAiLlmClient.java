package com.gentoro.onemcp.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.messages.ExecutionPlan;
import com.gentoro.onemcp.messages.StepImplementation;
import com.gentoro.onemcp.messages.Summary;
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

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {
    String modelId = configuration.getString("model", ChatModel.GPT_4_1.toString());
    ChatModel model;
    try {
      model = ChatModel.of(modelId);
    } catch (Exception e) {
      // fallback to GPT-4.1 if unknown
      model = ChatModel.GPT_4_1;
    }

    ChatCompletionCreateParams.Builder builder =
        ChatCompletionCreateParams.builder()
            .model(model);

    if( Message.contains( messages, Role.SYSTEM) ) {
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

    final TypeReference<HashMap<String, Object>> typeRef =
        new TypeReference<HashMap<String, Object>>() {};

    String result = null;
    main_loop:
    while (true) {
      long start = System.currentTimeMillis();
      ChatCompletion chatCompletion = openAIClient.chat().completions().create(builder.build());
      listener.on(EventType.ON_COMPLETION, chatCompletion);

      final List<ChatCompletionMessageToolCall> llmToolCalls = new ArrayList<>();
      ChatCompletion.Choice choice = chatCompletion.choices().getFirst();

      long end = System.currentTimeMillis();
      log.info(
          "[Inference] - OpenAI:\nLLM inference took {} ms.\nTotal tokens {}.\n---\n",
          (end - start),
          chatCompletion.usage().get().totalTokens());

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

            if (tool.name().equals(ExecutionPlan.class.getSimpleName())
                || tool.name().equals(StepImplementation.class.getSimpleName())
                || tool.name().equals(Summary.class.getSimpleName())) {
              break main_loop;
            }
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
    convertProperty(property).forEach((key, value) -> paramsBuilder.putAdditionalProperty(key, JsonValue.from(value)));
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
