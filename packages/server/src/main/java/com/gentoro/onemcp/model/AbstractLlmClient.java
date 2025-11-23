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
  private final OneMcp oneMcp;
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
    long start = System.currentTimeMillis();
    TelemetrySink t = telemetry();

    t.startChild("abstractLLM.generate");
    t.currentAttributes().put("message", message);
    t.currentAttributes()
        .put(
            "tools",
            Objects.requireNonNullElse(tools, Collections.<Tool>emptyList()).stream()
                .map(Tool::name)
                .collect(Collectors.joining(", ")));

    try {
      String content =
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
      t.endCurrentOk(
          Map.of("latencyMs", (System.currentTimeMillis() - start), "completion", content));
      return content;
    } catch (Exception e) {
      t.endCurrentError(
          Map.of(
              "latencyMs",
              (System.currentTimeMillis() - start),
              "error",
              ExceptionUtil.formatCompactStackTrace(e)));
      throw ExceptionUtil.rethrowIfUnchecked(
          e,
          (ex) ->
              new LlmException(
                  "There was a problem while running the inference with the chosen model.", ex));
    } finally {
      log.trace("generate() took {} ms", System.currentTimeMillis() - start);
      StdoutUtility.printRollingLine(
          oneMcp,
          "(Inference): completed in (%d)ms".formatted((System.currentTimeMillis() - start)));
    }
  }

  public abstract String runContentGeneration(
      String message, List<Tool> tools, InferenceEventListener listener);

  @Override
  public String chat(
      List<Message> messages,
      List<Tool> tools,
      boolean cacheable,
      final InferenceEventListener _listener) {
    StdoutUtility.printRollingLine(
        oneMcp, "(Inference): sending (%d) message(s) to LLM...".formatted(messages.size()));
    log.trace(
        "chat() called with: messages = [{}], tools = [{}], cacheable = [{}]",
        messages,
        Objects.requireNonNullElse(tools, Collections.<Tool>emptyList()).stream()
            .map(Tool::name)
            .collect(Collectors.joining(", ")),
        cacheable);
    long start = System.currentTimeMillis();
    try {
      // TODO: Implement the proper caching logic when possible.
      return runInference(
          messages,
          tools,
          new InferenceEventListener() {
            @Override
            public void on(EventType type, Object data) {
              if (_listener != null) {
                _listener.on(type, data);
              }
            }
          });
    } catch (Exception e) {
      throw ExceptionUtil.rethrowIfUnchecked(
          e,
          (ex) ->
              new LlmException(
                  "There was a problem while running the inference with the chosen model.", ex));
    } finally {
      log.trace("chat() took {} ms", System.currentTimeMillis() - start);
      StdoutUtility.printRollingLine(
          oneMcp,
          "(Inference): completed in (%d)ms".formatted((System.currentTimeMillis() - start)));
    }
  }

  public abstract String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener);

  public record Inference(List<Message> messages, String result) {}

  public record Cache(List<Inference> entries) {}
}
