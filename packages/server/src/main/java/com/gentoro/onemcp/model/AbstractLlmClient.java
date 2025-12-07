package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.LlmException;
import com.gentoro.onemcp.utility.StdoutUtility;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.Configuration;

/**
 * Base {@link LlmClient} with optional, naive file-based caching and common plumbing.
 *
 * <p>Subclasses implement {@link #runInference(List, List, InferenceEventListener)} to execute a
 * single turn with a concrete provider SDK. This class wraps that call with optional cache lookup
 * and persistence when enabled via configuration:
 *
 * <ul>
 *   <li>{@code llm.cache.enabled} (boolean, default false)
 *   <li>{@code llm.cache.location} (string, required if cache is enabled)
 * </ul>
 */
public abstract class AbstractLlmClient implements LlmClient {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(AbstractLlmClient.class);
  protected final Configuration configuration;
  protected final OneMcp oneMcp;
  private static final ThreadLocal<TelemetrySink> TELEMETRY_SINK = new ThreadLocal<>();

  public AbstractLlmClient(OneMcp oneMcp, Configuration configuration) {
    this.oneMcp = oneMcp;
    this.configuration = configuration;
  }

  @Override
  public TelemetryScope withTelemetry(TelemetrySink sink) {
    final TelemetrySink previous = TELEMETRY_SINK.get();
    TELEMETRY_SINK.set(sink);
    return () -> {
      // restore previous to support nesting
      if (previous == null) {
        TELEMETRY_SINK.remove();
      } else {
        TELEMETRY_SINK.set(previous);
      }
    };
  }

  protected TelemetrySink telemetry() {
    return TELEMETRY_SINK.get();
  }

  @Override
  public String generate(
      String message, List<Tool> tools, boolean cacheable, InferenceEventListener _listener) {
    StdoutUtility.printRollingLine(oneMcp, "(Inference): sending generate request to LLM...");
    log.trace(
        "generate() called with: message = [{}], tools = [{}], cacheable = [{}]",
        message,
        Objects.requireNonNullElse(tools, Collections.<Tool>emptyList()).stream()
            .map(Tool::name)
            .collect(Collectors.joining(", ")),
        cacheable);

    // Log input message for reporting
    if (oneMcp != null && oneMcp.inferenceLogger() != null) {
      List<Message> inputMessages = List.of(new Message(LlmClient.Role.USER, message));
      oneMcp.inferenceLogger().logLlmInputMessages(inputMessages);
    }

    long start = System.currentTimeMillis();
    TelemetrySink t = telemetry();
    if (t != null) {
      t.startChild("abstractLLM.generate");
      t.currentAttributes().put("message", message);
      t.currentAttributes()
          .put(
              "tools",
              Objects.requireNonNullElse(tools, Collections.<Tool>emptyList()).stream()
                  .map(Tool::name)
                  .collect(Collectors.joining(", ")));
    }

    String result = null;
    long promptTokens = 0;
    long completionTokens = 0;
    try {
      result =
          runContentGeneration(
              message,
              tools,
              new InferenceEventListener() {
                @Override
                public void on(EventType type, Object data) {
                  if (_listener != null) {
                    _listener.on(type, data);
                  }
                }
              });

      // Capture token usage from telemetry sink
      if (t != null) {
        Map<String, Object> attrs = t.currentAttributes();
        if (attrs != null) {
          Object pt = attrs.get("prompt.token");
          Object ct = attrs.get("completion.token");
          if (pt instanceof Number) promptTokens = ((Number) pt).longValue();
          if (ct instanceof Number) completionTokens = ((Number) ct).longValue();
        }
      }

      if (t != null) {
        t.endCurrentOk(
            Map.of("latencyMs", (System.currentTimeMillis() - start), "completion", result));
      }

      return result;
    } catch (Exception e) {
      if (t != null) {
        t.endCurrentError(
            Map.of(
                "latencyMs",
                (System.currentTimeMillis() - start),
                "error",
                ExceptionUtil.formatCompactStackTrace(e)));
      }
      throw ExceptionUtil.rethrowIfUnchecked(
          e,
          (ex) ->
              new LlmException(
                  "There was a problem while running the inference with the chosen model.", ex));
    } finally {
      long duration = System.currentTimeMillis() - start;
      log.trace("generate() took {} ms", duration);
      StdoutUtility.printRollingLine(
          oneMcp, "(Inference): completed in (%d)ms".formatted(duration));

      // Log LLM inference complete for reporting
      logInferenceComplete(duration, promptTokens, completionTokens, result);
    }
  }

  public abstract String runContentGeneration(
      String message, List<Tool> tools, InferenceEventListener listener);

  @Override
  public String chat(
      List<Message> messages,
      List<Tool> tools,
      boolean cacheable,
      final InferenceEventListener _listener,
      Float temperature) {
    StdoutUtility.printRollingLine(
        oneMcp, "(Inference): sending (%d) message(s) to LLM...".formatted(messages.size()));
    log.trace(
        "chat() called with: messages = [{}], tools = [{}], cacheable = [{}], temperature = [{}]",
        messages,
        Objects.requireNonNullElse(tools, Collections.<Tool>emptyList()).stream()
            .map(Tool::name)
            .collect(Collectors.joining(", ")),
        cacheable,
        temperature);

    // Log input messages for reporting (done here so all implementations log consistently)
    if (oneMcp != null && oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logLlmInputMessages(messages);
    }
    
    long start = System.currentTimeMillis();
    String result = null;
    long promptTokens = 0;
    long completionTokens = 0;
    try {
      // TODO: Implement the proper caching logic when possible.
      result =
          runInference(
              messages,
              tools,
              temperature,
              new InferenceEventListener() {
                @Override
                public void on(EventType type, Object data) {
                  if (_listener != null) {
                    _listener.on(type, data);
                  }
                }
              });

      // Capture token usage from telemetry sink
      TelemetrySink sink = telemetry();
      if (sink != null) {
        Map<String, Object> attrs = sink.currentAttributes();
        if (attrs != null) {
          Object pt = attrs.get("prompt.token");
          Object ct = attrs.get("completion.token");
          if (pt instanceof Number) promptTokens = ((Number) pt).longValue();
          if (ct instanceof Number) completionTokens = ((Number) ct).longValue();
        }
      }

      return result;
    } catch (Exception e) {
      throw ExceptionUtil.rethrowIfUnchecked(
          e,
          (ex) ->
              new LlmException(
                  "There was a problem while running the inference with the chosen model.", ex));
    } finally {
      long duration = System.currentTimeMillis() - start;
      log.trace("chat() took {} ms", duration);
      StdoutUtility.printRollingLine(
          oneMcp, "(Inference): completed in (%d)ms".formatted(duration));

      // Note: LLM inference complete logging is handled by concrete implementations
      // which have access to the actual response text and token counts
    }
  }

  public abstract String runInference(
      List<Message> messages, List<Tool> tools, Float temperature, InferenceEventListener listener);

  /**
   * Sets up telemetry for an LLM inference call.
   *
   * @param providerName The provider name (e.g., "anthropic", "gemini", "openai")
   * @param modelName The model name being used
   * @param toolsCount The number of tools available
   * @param messagesCount The number of messages in the request
   * @param mode The mode ("generate" or "chat")
   * @return The telemetry sink, or null if telemetry is not available
   */
  protected TelemetrySink setupTelemetry(
      String providerName, String modelName, int toolsCount, int messagesCount, String mode) {
    TelemetrySink t = telemetry();
    if (t != null) {
      t.startChild("llm." + providerName);
      t.currentAttributes().put("provider", providerName);
      t.currentAttributes().put("model", modelName);
      t.currentAttributes().put("tools.count", toolsCount);
      t.currentAttributes().put("messages.count", messagesCount);
      t.currentAttributes().put("mode", mode);
    }
    return t;
  }

  /**
   * Finishes telemetry for an LLM inference call and logs the completion.
   *
   * @param sink The telemetry sink (can be null)
   * @param duration The duration of the inference in milliseconds
   * @param promptTokens The number of prompt tokens used
   * @param completionTokens The number of completion tokens used
   * @param totalTokens The total number of tokens used (can be null)
   * @param responseText The response text from the LLM
   */
  protected void finishTelemetry(
      TelemetrySink sink,
      long duration,
      long promptTokens,
      long completionTokens,
      Long totalTokens,
      String responseText) {
    if (sink != null) {
      if (promptTokens > 0 || completionTokens > 0) {
        sink.addUsage(
            Long.valueOf(promptTokens),
            Long.valueOf(completionTokens),
            totalTokens != null ? totalTokens : Long.valueOf(promptTokens + completionTokens));
      }
      Map<String, Object> endAttrs = new java.util.HashMap<>();
      endAttrs.put("latencyMs", duration);
      if (totalTokens != null) {
        endAttrs.put("usage.total", totalTokens);
      }
      if (responseText != null) {
        endAttrs.put("response", responseText);
      }
      sink.endCurrentOk(endAttrs);
    }

    // Log LLM inference complete
    logInferenceComplete(duration, promptTokens, completionTokens, responseText);
  }

  /**
   * Detects the current phase from the telemetry sink attributes. Checks for explicit phase
   * attribute first, then falls back to span name detection.
   *
   * @param sink The telemetry sink to extract phase information from
   * @return The detected phase name, or "unknown" if not found
   */
  protected String detectPhase(TelemetrySink sink) {
    if (sink == null) {
      return "unknown";
    }

    Map<String, Object> attrs = sink.currentAttributes();
    if (attrs == null) {
      return "unknown";
    }

    // Check for phase in attributes first (set by OrchestratorTelemetrySink)
    Object phaseObj = attrs.get("phase");
    if (phaseObj != null) {
      return phaseObj.toString();
    }

    // Fall back to span name - use it directly if it looks like a phase
    // (not a provider name like "llm.anthropic" or operation names)
    String spanName = (String) attrs.get("span.name");
    if (spanName != null && !spanName.startsWith("llm.") && !spanName.startsWith("operation:")) {
      return spanName;
    }

    return "unknown";
  }

  /**
   * Logs a tool call to the inference logger if available.
   *
   * @param toolName The name of the tool being called
   * @param values The parameter values for the tool call
   */
  protected void logToolCall(String toolName, Map<String, Object> values) {
    if (oneMcp != null && oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logToolCall(toolName, values);
    }
  }

  /**
   * Logs tool output to the inference logger if available.
   *
   * @param toolName The name of the tool that produced the output
   * @param result The output result from the tool execution
   */
  protected void logToolOutput(String toolName, String result) {
    if (oneMcp != null && oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logToolOutput(toolName, result);
    }
  }

  /**
   * Logs LLM inference completion with phase detection and all metrics.
   *
   * @param duration The duration of the inference in milliseconds
   * @param promptTokens The number of prompt tokens used
   * @param completionTokens The number of completion tokens used
   * @param responseText The response text from the LLM
   */
  protected void logInferenceComplete(
      long duration, long promptTokens, long completionTokens, String responseText) {
    if (oneMcp != null && oneMcp.inferenceLogger() != null) {
      String phase = detectPhase(telemetry());
      // Check for cache hit status in telemetry attributes
      Boolean cacheHit = null;
      if (telemetry() != null && telemetry().currentAttributes() != null) {
        Object cacheHitObj = telemetry().currentAttributes().get("cacheHit");
        if (cacheHitObj instanceof Boolean) {
          cacheHit = (Boolean) cacheHitObj;
        }
      }
      oneMcp
          .inferenceLogger()
          .logLlmInferenceComplete(phase, duration, promptTokens, completionTokens, responseText, cacheHit);
    }
  }

  public record Inference(List<Message> messages, String result) {}

  public record Cache(List<Inference> entries) {}
}
