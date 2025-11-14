package com.gentoro.onemcp.model;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.LlmException;
import com.gentoro.onemcp.utility.StdoutUtility;
import java.util.Collections;
import java.util.List;
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

  public AbstractLlmClient(OneMcp oneMcp, Configuration configuration) {
    this.oneMcp = oneMcp;
    this.configuration = configuration;
  }

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
