package com.gentorox.services.agent;

import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.knowledgebase.KnowledgeBaseService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for AgentServiceConfig bean factory to ensure initialization validation.
 *
 * We avoid spinning a Spring context; instead we call the @Bean method directly,
 * providing mocked dependencies. This keeps the unit test fast and focused.
 */
public class AgentServiceConfigTest {

  @Test
  @DisplayName("Bean creation succeeds when foundation dir exists and is a directory")
  void beanCreationSuccess(@TempDir Path tmp) throws IOException {
    Files.createDirectories(tmp);

    AgentServiceConfig cfg = new AgentServiceConfig();
    InferenceService inference = mock(InferenceService.class);
    KnowledgeBaseService kb = mock(KnowledgeBaseService.class);

    AgentService service = cfg.agentService(tmp.toString(), inference, kb);
    assertNotNull(service);
    // service should be initialized; getConfig() should not throw
    assertDoesNotThrow(service::getConfig);
  }

  @Test
  @DisplayName("Bean creation fails with informative message when foundation dir is missing or not a directory")
  void beanCreationFailsForMissingDir(@TempDir Path tmp) {
    Path file = tmp.resolve("file.txt");
    try {
      Files.writeString(file, "x");
    } catch (IOException e) {
      fail(e);
    }

    AgentServiceConfig cfg = new AgentServiceConfig();
    InferenceService inference = mock(InferenceService.class);
    KnowledgeBaseService kb = mock(KnowledgeBaseService.class);

    // Non-existing path
    IllegalStateException ex1 = assertThrows(IllegalStateException.class, () ->
        cfg.agentService(tmp.resolve("missing").toString(), inference, kb));
    assertTrue(ex1.getMessage().contains("Foundation dir not found or not a directory"));

    // Existing but not a directory
    IllegalStateException ex2 = assertThrows(IllegalStateException.class, () ->
        cfg.agentService(file.toString(), inference, kb));
    assertTrue(ex2.getMessage().contains("Foundation dir not found or not a directory"));
  }
}
