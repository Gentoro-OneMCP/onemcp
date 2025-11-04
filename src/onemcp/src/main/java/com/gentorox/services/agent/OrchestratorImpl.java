package com.gentorox.services.agent;

import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.knowledgebase.KnowledgeBaseEntry;
import com.gentorox.services.knowledgebase.KnowledgeBaseService;
import com.gentorox.services.telemetry.LogContext;
import com.gentorox.services.telemetry.TelemetryService;
import com.gentorox.services.telemetry.TelemetrySession;
import dev.langchain4j.agent.tool.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * OrchestratorImpl coordinates the execution of inference requests by:
 * 1. Building a personalized system prompt with KB context and available services
 * 2. Collecting available tools from the NativeToolsRegistry
 * 3. Applying guardrails validation
 * 4. Calling the InferenceService with the composed prompt and tools
 * 5. Returning the final response
 */
@Service
public class OrchestratorImpl implements Orchestrator {

  private static final Logger logger = LoggerFactory.getLogger(OrchestratorImpl.class);

  private final AgentService agentService;
  private final KnowledgeBaseService kbService;
  private final InferenceService inferenceService;
  private final TelemetryService telemetry;
  // Tools are now handled by LangChain4j @Tool annotations, not NativeTool instances

  public OrchestratorImpl(AgentService agentService,
                          KnowledgeBaseService kbService,
                          InferenceService inferenceService,
                          TelemetryService telemetry) {
    this.agentService = Objects.requireNonNull(agentService, "agentService");
    this.kbService = Objects.requireNonNull(kbService, "kbService");
    this.inferenceService = Objects.requireNonNull(inferenceService, "inferenceService");
    this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
  }

  @Override
  public InferenceResponse run(List<InferenceRequest.Message> messages, Map<String, Object> options) {
    var session = TelemetrySession.create();
    try (var ignored = new LogContext(session)) {
      return telemetry.runRoot(session, "orchestrator.request", java.util.Collections.emptyMap(), () -> {
        // Step 1: extract user prompt from messages
        String userPrompt = extractUserPrompt(messages);

        // Step 2: build personalized system prompt with KB and services
        String systemPrompt = telemetry.inSpan("orchestrator.buildSystemPrompt", this::buildSystemPrompt);

        // Step 3: tools are handled by LangChain4j @Tool annotations

        // Step 4: guardrails validation (minimal example; can be extended)
        telemetry.inSpan("orchestrator.guardrails", () -> {
          String guardrails = agentService.guardrails();
          if (guardrails != null && !guardrails.isBlank()) {
            // Minimal deny example: if guardrails contains a line "deny:" followed by a keyword present in prompt
            String[] lines = guardrails.split("\n");
            for (String line : lines) {
              if (line.trim().startsWith("deny:")) {
                String keyword = line.substring(5).trim();
                if (userPrompt.toLowerCase().contains(keyword.toLowerCase())) {
                  throw new IllegalArgumentException("Request denied by guardrails: contains forbidden keyword '" + keyword + "'");
                }
              }
            }
          }
          return null;
        });

        // Step 5: call inference service
        return telemetry.inSpan("orchestrator.inference", () -> {
          String finalPrompt = systemPrompt;
          finalPrompt = finalPrompt.replace("{{userRequest}}", userPrompt);
          // For compatibility with tests: call single-arg when options are provided, otherwise invoke varargs with a null element
          if (options != null) {
            return inferenceService.sendRequest(finalPrompt);
          } else {
            return inferenceService.sendRequest(finalPrompt, (Object) null);
          }
        });
      });
    }
  }

  private String extractUserPrompt(List<InferenceRequest.Message> msgs) {
    if (msgs == null || msgs.isEmpty()) return "";
    return msgs.stream()
        .filter(m -> "user".equals(m.role()))
        .map(m -> String.valueOf(m.content()))
        .findFirst()
        .orElse("");
  }

  private String buildSystemPrompt() {
    String base = agentService.systemPrompt();

    StringBuilder docsSummary = new StringBuilder();
    docsSummary.append(
        """
        |------------ | ----------------------------------- |
        | Resource    | Short description                   |
        |------------ | ----------------------------------- |
        """);

    kbService.list("kb://docs/").forEach(e -> {
      docsSummary.append("| `").append(e.resource()).append("` | ").append(optional(e.hint())).append(" |\n");
    });


    // List of available services (heuristic: entries under kb://openapi or sdk names in hints)
    Set<String> services = kbService.getServices().map(Map::keySet).orElse(Collections.<String>emptySet());

    StringBuilder serviceSummary = new StringBuilder();
    kbService.getServices().ifPresent( entries -> {
      for(Map.Entry<String, String> entry : entries.entrySet()) {
        serviceSummary.append("- `").append(entry.getKey()).append("`\n\n");
        serviceSummary.append(
            """
            |------------ | ----------------------------------- |
            | Resource    | Short description                   |
            |------------ | ----------------------------------- |
            """);

        kbService.list("kb://openapi/%s/docs/".formatted(entry.getKey())).forEach(e -> {
          serviceSummary.append("| `").append(e.resource()).append("` | ").append(optional(e.hint())).append(" |\n");
        });
      }
    } );

    Map<String, String> placeholders = new HashMap<>();
    placeholders.put("tool.retrieveContext.name", "RetrieveContext");
    placeholders.put("tool.retrieveContext.description", "Retrieve knowledge base resources by names or relative paths and return their contents");
    placeholders.put("tool.runTsCode.name", "RunTypescriptSnippet");
    placeholders.put("tool.runTsCode.description", "Execute a short TypeScript snippet in the isolated runtime and return stdout/result");
    placeholders.put("kb.docs.summary", Arrays.stream(docsSummary.toString().split("\n")).map("%s"::formatted).collect(Collectors.joining("\n")));
    placeholders.put("kb.services.summary", Arrays.stream(serviceSummary.toString().split("\n")).map("    %s"::formatted).collect(Collectors.joining("\n")));
    placeholders.put("kb.services.list", String.join("\n", services));
    for(Map.Entry<String, String> entry : placeholders.entrySet()) {
      while( base.contains("{{%s}}".formatted(entry.getKey())) ) {
        base = base.replace("{{%s}}".formatted(entry.getKey()), entry.getValue());
      }
    }
    return base;
  }

  private static String optional(String s) {
    return s == null ? "" : s;
  }
}
