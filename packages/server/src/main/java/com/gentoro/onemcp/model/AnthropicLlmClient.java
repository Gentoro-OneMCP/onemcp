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

  private record InitializationContext(
      MessageCreateParams.Builder configBuilder,
      List<MessageParam> localMessages,
      String modelName) {}

  private InitializationContext initialize(List<Message> messages, List<Tool> tools) {
    String modelName = configuration.getString("model", Model.CLAUDE_4_SONNET_20250514.asString());
    MessageCreateParams.Builder configBuilder =
        MessageCreateParams.builder()
            .model(modelName)
            .maxTokens(configuration.getInt("max-tokens", 50000))
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

    return new InitializationContext(configBuilder, localMessages, modelName);
  }

  @Override
  public String runContentGeneration(
      String message, List<Tool> tools, InferenceEventListener listener) {
    InitializationContext ctx = initialize(Collections.emptyList(), tools);

    long start = System.currentTimeMillis();
    TelemetrySink t =
        setupTelemetry(
            "anthropic",
            ctx.modelName(),
            tools == null ? 0 : tools.size(),
            ctx.localMessages().size(),
            "generate");

    ctx.configBuilder().messages(ctx.localMessages());
    com.anthropic.models.messages.Message chatCompletion =
        anthropicClient.messages().create(ctx.configBuilder().build());
    if (listener != null) listener.on(EventType.ON_COMPLETION, chatCompletion);

    long end = System.currentTimeMillis();
    long promptTokens = chatCompletion.usage().inputTokens();
    long completionTokens = chatCompletion.usage().outputTokens();
    long totalTokens = promptTokens + completionTokens;

    // Extract response text
    String responseText = "";
    if (!chatCompletion.content().isEmpty()) {
      for (com.anthropic.models.messages.ContentBlock block : chatCompletion.content()) {
        if (block.isText()) {
          responseText += block.asText().text();
        }
      }
    }

    log.info(
        "[Inference] - Anthropic({}):\nLLM inference took {} ms.\nTotal tokens {}.\n---\n",
        ctx.modelName(),
        (end - start),
        totalTokens);

    finishTelemetry(t, (end - start), promptTokens, completionTokens, totalTokens, responseText);

    if (chatCompletion.content().isEmpty()) {
      throw new com.gentoro.onemcp.exception.LlmException(
          "No candidates returned from Anthropic inference.");
    }

    TextBlock textBlock =
        chatCompletion.content().stream()
            .filter(c -> c.isText() && c.text().isPresent())
            .map(c -> c.text().get())
            .findFirst()
            .orElseThrow(
                () ->
                    new com.gentoro.onemcp.exception.LlmException(
                        "No content returned from Anthropic inference."));

    if (textBlock.text().isEmpty()) {
      throw new com.gentoro.onemcp.exception.LlmException(
          "No content returned from Anthropic inference.");
    }

    return textBlock.text();
  }

  @Override
  public String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener) {
    InitializationContext ctx = initialize(Collections.emptyList(), tools);

    final List<ToolUseBlock> toolCalls = new ArrayList<>();
    com.anthropic.models.messages.Message chatCompletions;
    do {
      toolCalls.clear();

      long start = System.currentTimeMillis();
      ctx.configBuilder().messages(ctx.localMessages());
      chatCompletions = anthropicClient.messages().create(ctx.configBuilder().build());
      listener.on(EventType.ON_COMPLETION, chatCompletions);

      long end = System.currentTimeMillis();
      long promptTokens = chatCompletions.usage().inputTokens();
      long completionTokens = chatCompletions.usage().outputTokens();
      long totalTokens = promptTokens + completionTokens;

      // Extract response text
      String responseText = "";
      for (com.anthropic.models.messages.ContentBlock block : chatCompletions.content()) {
        if (block.isText()) {
          responseText += block.asText().text();
        }
      }

      log.info(
          "[Inference] - Anthropic({}):\nLLM inference took {} ms.\nTotal tokens {}.\n---\n",
          ctx.modelName(),
          (end - start),
          totalTokens);

      // Log LLM inference complete with response
      logInferenceComplete((end - start), promptTokens, completionTokens, responseText);

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

          // Log tool call
          logToolCall(tool.name(), values);

          String result = tool.execute(values);

          // Log tool output
          logToolOutput(tool.name(), result);

          ctx.localMessages()
              .add(
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

          ctx.localMessages()
              .add(
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
    listener.on(EventType.ON_END, ctx.localMessages());
    return ctx.localMessages().stream()
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
    convertProperty(property)
        .forEach((key, value) -> builder.putAdditionalProperty(key, JsonValue.from(value)));
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
