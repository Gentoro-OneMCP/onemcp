package com.gentoro.onemcp.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.exception.OneMcpException;
import com.gentoro.onemcp.exception.StateException;
import com.gentoro.onemcp.exception.ValidationException;
import com.gentoro.onemcp.messages.Documentation;
import com.gentoro.onemcp.messages.JsonSchemaGenerator;
import com.gentoro.onemcp.messages.YamlTemplateGenerator;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.model.Tool;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractServiceImpl<Type> {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(AbstractServiceImpl.class);

  private enum Mode {
    TOOL,
    TEMPLATE
  }

  private final OrchestratorContext context;
  private PromptTemplate activeTemplate;
  private PromptTemplate.PromptSession activeSession;
  private final Class<Type> type;
  private Mode mode = Mode.TOOL;

  public AbstractServiceImpl(OrchestratorContext context, Class<Type> type) {
    this.context = context;
    this.type = type;
  }

  public OrchestratorContext context() {
    return this.context;
  }

  public AbstractServiceImpl<Type> newSession(String path) {
    this.activeTemplate = context.oneMcp().promptRepository().get(path);
    this.activeSession = activeTemplate.newSession();
    return this;
  }

  public AbstractServiceImpl<Type> enableOnly(String session, Object... values) {
    this.activeSession.enableOnly(session, kv(values));
    return this;
  }

  public AbstractServiceImpl<Type> enableIf(boolean condition, String session, Object... values) {
    if (condition) {
      enable(session, values);
    }
    return this;
  }

  public String renderText() {
    return activeSession.renderText();
  }

  public AbstractServiceImpl<Type> enable(String session, Object... values) {
    this.activeSession.enable(session, kv(values));
    return this;
  }

  // Convenience: varargs key/value → map
  private Map<String, Object> kv(Object... values) {
    if (values == null || values.length == 0) return Collections.emptyMap();
    if (values.length % 2 != 0) {
      throw new ValidationException("Values must be key/value pairs");
    }
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < values.length; i += 2) {
      if (!(values[i] instanceof String)) {
        throw new ValidationException("Key at index " + i + " must be a String");
      }
      m.put((String) values[i], values[i + 1]);
    }
    m.put("mode", mode.toString());
    if (mode == Mode.TEMPLATE) {
      m.put("typeTemplate", YamlTemplateGenerator.generateYaml(this.type));
      if (!this.type.equals(Documentation.class)) {
        m.put("docTemplate", YamlTemplateGenerator.generateYaml(Documentation.class));
      }
    }
    return m;
  }

  public Type chat() {
    List<LlmClient.Message> messages = new ArrayList<>(activeSession.renderMessages());
    int attempts = 0;
    final AtomicReference<Type> capturedOutput = new AtomicReference<>();
    while (++attempts < 4) {
      AtomicBoolean toolCall = new AtomicBoolean(false);
      String result =
          context()
              .llmClient()
              .chat(
                  messages,
                  mode == Mode.TEMPLATE
                      ? Collections.emptyList()
                      : List.of(
                          JsonSchemaGenerator.<Type>asCapturingTool(
                              type,
                              (output) -> {
                                capturedOutput.set(output);
                                return "Well done, you can complete the process know, have all needed information.";
                              }),
                          JsonSchemaGenerator.<Documentation>asCapturingTool(
                              Documentation.class,
                              (doc) -> {
                                return context
                                    .oneMcp()
                                    .promptRepository()
                                    .get("/documentation-request")
                                    .newSession()
                                    .enableOnly(
                                        "request",
                                        Map.of(
                                            "services",
                                            context.oneMcp().knowledgeBase().services().stream()
                                                .filter(
                                                    srv -> doc.services().contains(srv.getSlug()))
                                                .toList()))
                                    .renderText();
                              })),
                  false,
                  (eventType, data) -> {
                    if (eventType == LlmClient.EventType.ON_TOOL_CALL) {
                      Tool calledTool = (Tool) data;
                      if (calledTool.definition().name().equals(type.getSimpleName())) {
                        toolCall.set(true);
                      }
                    }
                  });

      if (mode == Mode.TOOL) {
        if (capturedOutput.get() == null) {
          if (!toolCall.get()) {
            // expected a tool call that did not happen, trying again.
            log.warn(
                "Flow expected the tool {} to be called, but it was not. Trying again...",
                type.getSimpleName());
            messages.add(new LlmClient.Message(LlmClient.Role.ASSISTANT, result));
            messages.add(
                new LlmClient.Message(
                    LlmClient.Role.USER,
                    "Calling the Function / Tool %s is not optional, review the provided instructions and proceed with the process."
                        .formatted(type.getSimpleName())));
            continue;
          }
        } else {
          return capturedOutput.get();
        }
      }

      messages.add(new LlmClient.Message(LlmClient.Role.ASSISTANT, result));
      try {

        String jsonStr = StringUtility.extractSnippet(result, "yaml");
        if (jsonStr == null || jsonStr.isEmpty()) {
          throw new StateException("No YAML snippet found in response");
        }

        JsonNode jsonNode = JacksonUtility.getYamlMapper().readTree(jsonStr);
        if (!jsonNode.has("message")) {
          throw new StateException("Missing `message` property");
        }

        if (!jsonNode.get("message").has("type")) {
          throw new StateException("Missing `type` property");
        }

        if (!jsonNode.get("message").get("type").asText().equals(type.getSimpleName())) {

          if (!this.type.equals(Documentation.class)
              && jsonNode
                  .get("message")
                  .get("type")
                  .asText()
                  .equals(Documentation.class.getSimpleName())) {
            Documentation doc =
                JacksonUtility.getJsonMapper()
                    .convertValue(jsonNode.get("message").get("data"), Documentation.class);
            messages.add(
                new LlmClient.Message(
                    LlmClient.Role.USER,
                    context
                        .oneMcp()
                        .promptRepository()
                        .get("/documentation-request")
                        .newSession()
                        .enableOnly(
                            "request",
                            Map.of(
                                "services",
                                context.oneMcp().knowledgeBase().services().stream()
                                    .filter(srv -> doc.services().contains(srv.getSlug()))
                                    .toList()))
                        .renderText()));
            continue;
          }

          throw new ExecutionException(
              "Unexpected message type: " + jsonNode.get("message").get("type").asText());
        }

        return type.cast(
            JacksonUtility.getJsonMapper().convertValue(jsonNode.get("message").get("data"), type));

      } catch (OneMcpException ex) {
        log.warn(
            "Generated content did not match expected format ({}), re-trying one more time.\n{}---\n{}---\n",
            type.getSimpleName(),
            ex.getMessage(),
            StringUtility.formatWithIndent(result, 4));

        messages.add(
            new LlmClient.Message(
                LlmClient.Role.USER,
                context
                    .oneMcp()
                    .promptRepository()
                    .get("/invalid-message")
                    .newSession()
                    .enableOnly(
                        "retry",
                        Map.of(
                            "errors",
                            ex.getMessage(),
                            "typeTemplate",
                            YamlTemplateGenerator.generateYaml(type)))
                    .renderText()));
      } catch (JsonProcessingException ex) {
        // Capture full exception info
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        log.warn(
            "Generated content did not match expected format ({}), re-trying one more time.\n{}---\n{}---\n",
            type.getSimpleName(),
            sw.toString(),
            StringUtility.formatWithIndent(result, 4));
        messages.add(
            new LlmClient.Message(
                LlmClient.Role.USER,
                context
                    .oneMcp()
                    .promptRepository()
                    .get("/invalid-message")
                    .newSession()
                    .enableOnly(
                        "retry",
                        Map.of(
                            "errors",
                            sw.toString(),
                            "typeTemplate",
                            YamlTemplateGenerator.generateYaml(type)))
                    .renderText()));
      }
    }

    throw new ExecutionException(
        "Aborted after %d attempts.".formatted(attempts));
  }
}
