package com.gentoro.onemcp.indexing.docs;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.model.agent.Alias;
import com.gentoro.onemcp.handbook.model.agent.Entity;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.model.Tool;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EntityExtractorTest {

  static class CapturingLlm implements LlmClient {
    String lastPrompt;
    String nextResponse;

    @Override
    public String chat(
        List<LlmClient.Message> messages,
        List<Tool> tools,
        boolean cacheable,
        LlmClient.InferenceEventListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String generate(
        String message,
        List<Tool> tools,
        boolean cacheable,
        LlmClient.InferenceEventListener listener) {
      this.lastPrompt = message;
      return nextResponse;
    }

    @Override
    public TelemetryScope withTelemetry(TelemetrySink sink) {
      return () -> {};
    }
  }

  static class TestMcp extends OneMcp {
    private final LlmClient llm;

    TestMcp(LlmClient llm) {
      super(new String[] {});
      this.llm = llm;
    }

    @Override
    public LlmClient llmClient() {
      return llm;
    }

    @Override
    public Configuration configuration() {
      return new YAMLConfiguration();
    }
  }

  private static Entity entity(String name, String description, String... aliasTerms) {
    Entity e = new Entity();
    e.setName(name);
    if (description != null) e.setDescription(description);
    List<Alias> aliases = new ArrayList<>();
    if (aliasTerms != null && aliasTerms.length > 0) {
      Alias a = new Alias();
      a.setTerms(java.util.Arrays.asList(aliasTerms));
      aliases.add(a);
    }
    e.setAliases(aliases);
    return e;
  }

  @Test
  @DisplayName("Parses valid JSON from LLM into ChunkEntityExtraction")
  void parsesValidJson() {
    CapturingLlm llm = new CapturingLlm();
    TestMcp mcp = new TestMcp(llm);

    EntityExtractor extractor = new EntityExtractor(mcp, List.of(entity("Widget", "desc")));
    Chunk chunk = new Chunk("c-1", "file.md", "Sec", "Widget is defined here.");

    llm.nextResponse =
        "{\n"
            + "  \"chunkId\": \"c-1\",\n"
            + "  \"matches\": [{\"entity\": \"Widget\", \"confidence\": 0.8, \"reason\": \"mentioned\"}]\n"
            + "}";

    ChunkEntityExtraction cee = extractor.extract(chunk);
    assertEquals("c-1", cee.getChunkId());
    assertNotNull(cee.getMatches());
    assertEquals(1, cee.getMatches().size());
    assertEquals("Widget", cee.getMatches().get(0).getEntity());
    assertEquals(0.8, cee.getMatches().get(0).getConfidence(), 1e-6);
    assertEquals("mentioned", cee.getMatches().get(0).getReason());

    // Prompt should contain entity name, chunk id, and content
    assertTrue(llm.lastPrompt.contains("Widget"));
    assertTrue(llm.lastPrompt.contains("ChunkId: c-1"));
    assertTrue(llm.lastPrompt.contains("Widget is defined here."));
  }

  @Test
  @DisplayName("Falls back to naive matching when JSON invalid; matches names and aliases")
  void fallbackNaiveMatching() {
    CapturingLlm llm = new CapturingLlm();
    TestMcp mcp = new TestMcp(llm);

    Entity e1 = entity("Gadget", null, "device", "appliance");
    Entity e2 = entity("Widget", null);
    EntityExtractor extractor = new EntityExtractor(mcp, List.of(e1, e2));

    Chunk chunk = new Chunk("c-2", "file.md", "Sec", "This device works with a widget.");

    // Force parse failure
    llm.nextResponse = "not-json";

    ChunkEntityExtraction cee = extractor.extract(chunk);
    assertEquals("c-2", cee.getChunkId());
    assertNotNull(cee.getMatches());
    // Both Gadget (via alias 'device') and Widget (by name) should be found
    var names = cee.getMatches().stream().map(EntityMatch::getEntity).toList();
    assertTrue(names.contains("Gadget"));
    assertTrue(names.contains("Widget"));

    for (EntityMatch m : cee.getMatches()) {
      assertTrue(m.getConfidence() > 0.5);
      assertNotNull(m.getReason());
      assertFalse(m.getReason().isBlank());
    }

    // Prompt should list entity aliases in human-readable lines
    assertTrue(llm.lastPrompt.contains("Gadget"));
    assertTrue(llm.lastPrompt.contains("aliases: device, appliance"));
  }
}
