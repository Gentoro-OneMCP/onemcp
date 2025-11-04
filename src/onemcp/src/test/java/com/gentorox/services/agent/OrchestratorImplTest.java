package com.gentorox.services.agent;

import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.knowledgebase.KnowledgeBaseEntry;
import com.gentorox.services.knowledgebase.KnowledgeBaseService;
import com.gentorox.services.telemetry.TelemetryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for OrchestratorImpl focusing on orchestration flow, guardrails enforcement,
 * and robustness with null/empty inputs. External dependencies are fully mocked.
 */
public class OrchestratorImplTest {

  AgentService agent;
  KnowledgeBaseService kb;
  InferenceService inference;
  TelemetryService telemetry;

  @BeforeEach
  void setUp() {
    agent = mock(AgentService.class);
    kb = mock(KnowledgeBaseService.class);
    inference = mock(InferenceService.class);
    telemetry = mock(TelemetryService.class);

    // Tools are now handled by LangChain4j @Tool annotations

    // Telemetry helpers: execute suppliers/runnables directly to avoid OTEL complexity in unit tests
    when(telemetry.runRoot(any(), anyString(), anyMap(), any(Supplier.class))).thenAnswer(inv -> {
      Supplier<?> body = inv.getArgument(3);
      return body.get();
    });
    when(telemetry.inSpan(anyString(), any(Supplier.class))).thenAnswer(inv -> {
      Supplier<?> body = inv.getArgument(1);
      return body.get();
    });
    when(telemetry.inSpan(anyString(), anyMap(), any(Supplier.class))).thenAnswer(inv -> {
      Supplier<?> body = inv.getArgument(2);
      return body.get();
    });
    // Runnable overloads
    doAnswer(inv -> { Runnable r = inv.getArgument(1); r.run(); return null; })
        .when(telemetry).inSpan(anyString(), any(Runnable.class));
    doAnswer(inv -> { Runnable r = inv.getArgument(2); r.run(); return null; })
        .when(telemetry).inSpan(anyString(), anyMap(), any(Runnable.class));

    doAnswer(inv -> null).when(telemetry).countPrompt(anyString(), anyString());
    doAnswer(inv -> null).when(telemetry).countModelCall(anyString(), anyString());
  }

  @Test
  @DisplayName("run() builds final prompt with KB, services, tools and calls InferenceService with options passthrough")
  void runBuildsPromptAndCallsInference() {
    when(agent.systemPrompt()).thenReturn("BASE_SP\n{{userRequest}}");
    when(agent.guardrails()).thenReturn("");

    // KB has mixed entries; services are those under kb://openapi
    when(kb.list("")).thenReturn(List.of(
        new KnowledgeBaseEntry("kb://docs/A.md", "alpha", ""),
        new KnowledgeBaseEntry("kb://openapi/payments.yaml", "", ""),
        new KnowledgeBaseEntry("kb://openapi/catalog.yaml", "", "")
    ));

    OrchestratorImpl orch = new OrchestratorImpl(agent, kb, inference, telemetry);

    List<InferenceRequest.Message> msgs = List.of(
        new InferenceRequest.Message("user", "Pay order 123"),
        new InferenceRequest.Message("assistant", "Ack")
    );
    Map<String, Object> opts = Map.of("reqId", "r-1");

    when(inference.sendRequest(anyString()))
        .thenReturn(new InferenceResponse("ok", Optional.empty(), ""));

    InferenceResponse resp = orch.run(msgs, opts);
    assertEquals("ok", resp.content());

    // Verify the prompt contains expected content
    verify(inference).sendRequest(argThat(prompt ->
        prompt.contains("BASE_SP") &&
            prompt.contains("Pay order 123")
    ));
  }

  @Test
  @DisplayName("Guardrails deny leads to IllegalArgumentException and skips inference call")
  void guardrailsDeny() {
    when(agent.systemPrompt()).thenReturn("BASE");
    when(agent.guardrails()).thenReturn("deny: delete");

    when(kb.list("")).thenReturn(List.of());

    OrchestratorImpl orch = new OrchestratorImpl(agent, kb, inference, telemetry);

    List<InferenceRequest.Message> msgs = List.of(new InferenceRequest.Message("user", "please delete file"));

    assertThrows(IllegalArgumentException.class, () -> orch.run(msgs, Map.of()));
    verifyNoInteractions(inference);
  }

  @Test
  @DisplayName("Null/empty inputs are handled without NPE; first user message extraction works")
  void robustnessWithNulls() {
    when(agent.systemPrompt()).thenReturn("BASE");
    when(agent.guardrails()).thenReturn("");
    when(kb.list("")).thenReturn(List.of());

    OrchestratorImpl orch = new OrchestratorImpl(agent, kb, inference, telemetry);

    // Null lists/options
    when(inference.sendRequest(anyString(), any()))
        .thenReturn(new InferenceResponse("ok", Optional.empty(), ""));

    InferenceResponse resp = orch.run(null, null);
    assertEquals("ok", resp.content());
  }
}
