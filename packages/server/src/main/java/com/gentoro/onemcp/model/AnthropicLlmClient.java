package com.gentoro.onemcp.model;

import com.anthropic.client.AnthropicClient;
import com.anthropic.core.JsonValue;
import com.anthropic.models.messages.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

/**
 * Minimal Gemini implementation placeholder. For now it does not support tool-calling. It can be
 * extended to integrate with Google's Generative AI Java SDK.
 */
public class AnthropicLlmClient extends AbstractLlmClient {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(AnthropicLlmClient.class);
  private final AnthropicClient anthropicClient;

  public AnthropicLlmClient(
      OneMcp oneMcp, AnthropicClient anthropicClient, Configuration configuration) {
    super(oneMcp, configuration);
    this.anthropicClient = anthropicClient;
  }

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {
    String modelName = configuration.getString("model", Model.CLAUDE_4_SONNET_20250514.asString());
    MessageCreateParams.Builder configBuilder =
        MessageCreateParams.builder()
            .model(modelName)
            .maxTokens(50000)
            .toolChoice(
                ToolChoice.ofAny(ToolChoiceAny.builder().disableParallelToolUse(true).build()))
            .system(Message.findFirst(messages, Role.SYSTEM).content());

    if (!tools.isEmpty()) {
      tools.stream().map(tool -> convertTool(tool.definition())).forEach(configBuilder::addTool);
    }

    List<MessageParam> localMessages = new ArrayList<>();
    Message.allExcept(messages, Role.SYSTEM)
        .forEach(
            message ->
                localMessages.add(
                    MessageParam.builder()
                        .role(
                            switch (message.role()) {
                              case USER -> MessageParam.Role.USER;
                              case ASSISTANT -> MessageParam.Role.ASSISTANT;
                              default -> throw new com.gentoro.onemcp.exception.StateException(
                                  "Unknown message role: " + message.role());
                            })
                        .content(message.content())
                        .build()));

    final List<ToolUseBlock> toolCalls = new ArrayList<>();
    com.anthropic.models.messages.Message chatCompletions;
    do {
      toolCalls.clear();

      long start = System.currentTimeMillis();
      configBuilder.messages(localMessages);
      chatCompletions = anthropicClient.messages().create(configBuilder.build());
      listener.on(EventType.ON_COMPLETION, chatCompletions);

      long end = System.currentTimeMillis();
      long totalTokens =
          chatCompletions.usage().inputTokens() + chatCompletions.usage().outputTokens();
      log.info(
          "[Inference] - Anthropic({}):\nLLM inference took {} ms.\nTotal tokens {}.\n---\n",
          modelName,
          (end - start),
          totalTokens);

      // TODO: Report usage stats
      /**
       * Statistics.getInstance().increment( (int) chatCompletions.usage().inputTokens(), (int)
       * chatCompletions.usage().outputTokens());
       */

      // Iterate through the content blocks
      chatCompletions
          .content()
          .forEach(
              contentBlock -> {
                if (contentBlock.isToolUse()) {
                  toolCalls.add(contentBlock.asToolUse());
                }
              });

      final TypeReference<HashMap<String, Object>> toolCallTypeRef =
          new TypeReference<HashMap<String, Object>>() {};
      for (ToolUseBlock toolCall : toolCalls) {
        Tool tool =
            tools.stream()
                .filter(t -> t.name().equals(toolCall.name()))
                .findFirst()
                .orElseThrow(
                    () -> {
                      return new RuntimeException(
                          "Tool not found: "
                              + toolCall.name()
                              + ", the tools available are: "
                              + tools.stream().map(Tool::name).collect(Collectors.joining(", ")));
                    });
        listener.on(EventType.ON_TOOL_CALL, tool);
        try {
          Map<String, Object> values = new HashMap<>();
          Objects.requireNonNull(toolCall._input().convert(toolCallTypeRef))
              .forEach((key, value) -> values.put(key, value.toString()));
          String result = tool.execute(values);

          localMessages.add(
              MessageParam.builder()
                  .role(MessageParam.Role.USER)
                  .contentOfBlockParams(
                      List.of(
                          ContentBlockParam.ofToolResult(
                              ToolResultBlockParam.builder()
                                  .toolUseId(toolCall.id())
                                  .content(result)
                                  .build())))
                  .build());
        } catch (Exception toolExecError) {
          String errorDetails;
          try {
            errorDetails =
                JacksonUtility.getJsonMapper()
                    .writeValueAsString(
                        Map.of(
                            "isError",
                            true,
                            "errorMessage",
                            "Error executing function call: "
                                + toolCall.name()
                                + ", understand the error and report back with the most appropriate context.",
                            "errorDetails",
                            toolExecError.getMessage()));
          } catch (Exception e) {
            errorDetails =
                "Error executing function call: "
                    + toolCall.name()
                    + ", understand the error and report back with the most appropriate context.";
          }

          localMessages.add(
              MessageParam.builder()
                  .role(MessageParam.Role.USER)
                  .contentOfBlockParams(
                      List.of(
                          ContentBlockParam.ofToolResult(
                              ToolResultBlockParam.builder()
                                  .toolUseId(toolCall.id())
                                  .content(errorDetails)
                                  .build())))
                  .build());
        }
      }
    } while (chatCompletions.stopReason().isPresent() && toolCalls.isEmpty());
    listener.on(EventType.ON_END, localMessages);
    return localMessages.stream()
        .filter(m -> m.content().isString())
        .toList()
        .getLast()
        .content()
        .asString();
  }

  private Map<String, Object> convertProperty(ToolProperty property) {
    Map<String, Object> result = new HashMap<>();
    result.put("type", asAnthropicType(property.getType()));
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

  private com.anthropic.models.messages.Tool.InputSchema convertSchema(ToolProperty property) {
    com.anthropic.models.messages.Tool.InputSchema.Builder builder =
        com.anthropic.models.messages.Tool.InputSchema.builder()
            .type(asAnthropicType(property.getType()));
    convertProperty(property).forEach((key, value) -> builder.putAdditionalProperty(key, JsonValue.from(value)));
    return builder.build();
  }

  private JsonValue asAnthropicType(ToolProperty.Type type) {
    return switch (type) {
      case OBJECT -> JsonValue.from("object");
      case NUMBER -> JsonValue.from("number");
      case ARRAY -> JsonValue.from("array");
      case BOOLEAN -> JsonValue.from("boolean");
      default -> JsonValue.from("string");
    };
  }

  private com.anthropic.models.messages.Tool convertTool(ToolDefinition def) {
    return com.anthropic.models.messages.Tool.builder()
        .name(def.name())
        .description(def.description())
        .inputSchema(convertSchema(def.schema()))
        .build();
  }
}
