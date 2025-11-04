package com.gentorox.services.knowledgebase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class FileKnowledgeBasePersistenceTest {

  @Test
  void loadReturnsEmptyWhenFileDoesNotExist(@TempDir Path tmp) throws Exception {
    FileKnowledgeBasePersistence p = new FileKnowledgeBasePersistence();
    Path file = tmp.resolve("kb-state.json");
    assertFalse(Files.exists(file));
    Optional<KnowledgeBaseState> st = p.load(file);
    assertTrue(st.isEmpty());
  }

  @Test
  void saveAndLoadRoundTrip(@TempDir Path tmp) throws Exception {
    FileKnowledgeBasePersistence p = new FileKnowledgeBasePersistence();
    Path file = tmp.resolve("kb/knowledge-base-state.json");

    KnowledgeBaseEntry e1 = new KnowledgeBaseEntry("file:///a.md", "doc a", "doc a");
    KnowledgeBaseEntry e2 = new KnowledgeBaseEntry("mem://b.md", "doc b", "doc b");
    KnowledgeBaseState state = new KnowledgeBaseState("sig-123", List.of(e1, e2), Collections.emptyMap());

    p.save(file, state);
    assertTrue(Files.exists(file));

    Optional<KnowledgeBaseState> loaded = p.load(file);
    assertTrue(loaded.isPresent());
    assertEquals("sig-123", loaded.get().signature());
    assertEquals(2, loaded.get().entries().size());
    assertEquals("file:///a.md", loaded.get().entries().get(0).resource());
  }
}
