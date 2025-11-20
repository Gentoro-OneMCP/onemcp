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
  protected final OneMcp oneMcp;

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
    
    // Log LLM inference start
    int toolCount = tools != null ? tools.size() : 0;
    oneMcp.inferenceLogger().logLlmInferenceStart(messages.size(), toolCount);
    
    long start = System.currentTimeMillis();
    final java.util.concurrent.atomic.AtomicReference<Integer> tokensUsedRef = 
        new java.util.concurrent.atomic.AtomicReference<Integer>(null);
    String result = null;
    try {
      // TODO: Implement the proper caching logic when possible.
      result = runInference(
          messages,
          tools,
          new InferenceEventListener() {
            @Override
            public void on(EventType type, Object data) {
              if (_listener != null) {
                _listener.on(type, data);
              }
              // Capture token usage from completion events
              if (type == EventType.ON_COMPLETION && data != null) {
                Integer tokens = extractTokenCount(data);
                if (tokens != null) {
                  tokensUsedRef.set(tokens);
                }
              }
              // Note: Tool calls are logged in concrete implementations where arguments are available
            }
          });
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
          oneMcp,
          "(Inference): completed in (%d)ms".formatted(duration));
      
      // Log LLM inference completion
      oneMcp.inferenceLogger().logLlmInferenceComplete(
          duration, tokensUsedRef.get(), result);
    }
  }

  /**
   * Extract token count from provider-specific completion response objects.
   * Subclasses can override this to extract tokens from their specific response types.
   *
   * @param completionData The completion event data (provider-specific)
   * @return Token count if extractable, null otherwise
   */
  protected Integer extractTokenCount(Object completionData) {
    // Default implementation: try to extract via reflection for common patterns
    if (completionData == null) {
      return null;
    }
    
    try {
      // Try OpenAI pattern: usage().get().totalTokens()
      java.lang.reflect.Method getUsage = completionData.getClass().getMethod("usage");
      Object usage = getUsage.invoke(completionData);
      if (usage != null) {
        java.lang.reflect.Method getTotalTokens = usage.getClass().getMethod("totalTokens");
        Object totalTokens = getTotalTokens.invoke(usage);
        if (totalTokens instanceof Integer) {
          return (Integer) totalTokens;
        } else if (totalTokens instanceof Long) {
          return ((Long) totalTokens).intValue();
        }
      }
    } catch (Exception e) {
      // Try Gemini pattern: usageMetadata().get().totalTokenCount().get()
      try {
        java.lang.reflect.Method getUsageMetadata = completionData.getClass().getMethod("usageMetadata");
        Object usageMetadata = getUsageMetadata.invoke(completionData);
        if (usageMetadata != null) {
          java.lang.reflect.Method getTotalTokenCount = usageMetadata.getClass().getMethod("totalTokenCount");
          Object totalTokenCount = getTotalTokenCount.invoke(usageMetadata);
          if (totalTokenCount != null) {
            java.lang.reflect.Method getValue = totalTokenCount.getClass().getMethod("get");
            Object value = getValue.invoke(totalTokenCount);
            if (value instanceof Integer) {
              return (Integer) value;
            } else if (value instanceof Long) {
              return ((Long) value).intValue();
            }
          }
        }
      } catch (Exception e2) {
        // Not extractable via reflection, subclasses should override
      }
    }
    
    return null;
  }

  public abstract String runInference(
      List<Message> messages, List<Tool> tools, InferenceEventListener listener);

  public record Inference(List<Message> messages, String result) {}

  public record Cache(List<Inference> entries) {}
}
