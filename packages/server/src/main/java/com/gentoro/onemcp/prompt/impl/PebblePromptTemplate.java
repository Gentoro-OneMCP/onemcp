package com.gentoro.onemcp.prompt.impl;

import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptTemplate;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.extension.AbstractExtension;
import io.pebbletemplates.pebble.extension.Function;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Pebble-based implementation of an immutable PromptTemplate definition. Rendering state is
 * isolated in PromptSession instances.
 */
public class PebblePromptTemplate implements PromptTemplate {
  private static final PebbleEngine ENGINE =
      new PebbleEngine.Builder()
          .strictVariables(true)
          .extension(
              new AbstractExtension() {
                @Override
                public Map<String, Function> getFunctions() {
                  return Map.of(
                      "call", new ReflectiveCallFunction(),
                      "ident", new IdentFunction());
                }
              })
          .build();

  private final String id;
  private final List<PromptSection> sections;
  private final List<CompiledSection> compiled;

  private record CompiledSection(PromptSection section, PebbleTemplate template) {}

  public PebblePromptTemplate(String id, List<PromptSection> sections) {
    this.id = Objects.requireNonNull(id, "id");
    this.sections = List.copyOf(Objects.requireNonNull(sections, "sections"));
    this.compiled =
        this.sections.stream()
            .map(s -> new CompiledSection(s, ENGINE.getLiteralTemplate(s.content())))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public List<PromptSection> sections() {
    return sections; // already unmodifiable
  }

  @Override
  public PromptSession newSession() {
    return new Session();
  }

  private class Session implements PromptSession {
    private final Map<String, Map<String, Object>> enabled = new HashMap<>();

    Session() {
      // initialize defaults
      resetToDefaults();
    }

    @Override
    public PromptSession enable(String sectionId, Map<String, Object> vars) {
      enabled.put(sectionId, vars != null ? new HashMap<>(vars) : new HashMap<>());
      return this;
    }

    @Override
    public PromptSession enableOnly(String sectionId, Map<String, Object> vars) {
      clear();
      return enable(sectionId, vars);
    }

    @Override
    public PromptSession disable(String... sectionIds) {
      if (sectionIds != null) {
        for (String id : sectionIds) {
          enabled.remove(id);
        }
      }
      return this;
    }

    @Override
    public PromptSession clear() {
      enabled.clear();
      return this;
    }

    @Override
    public PromptSession resetToDefaults() {
      clear();
      for (PromptSection s : sections) {
        if (s.enabledByDefault()) {
          enable(s.id(), Map.of());
        }
      }
      return this;
    }

    @Override
    public List<LlmClient.Message> renderMessages() {
      List<LlmClient.Message> out = new ArrayList<>();
      for (CompiledSection cs : compiled) {
        PromptSection s = cs.section();
        if (enabled.containsKey(s.id())) {
          try {
            Map<String, Object> ctx = Optional.ofNullable(enabled.get(s.id())).orElseGet(Map::of);
            Writer writer = new StringWriter();
            cs.template().evaluate(writer, ctx);
            out.add(new LlmClient.Message(s.role(), writer.toString()));
          } catch (Exception e) {
            throw new com.gentoro.onemcp.exception.PromptException(
                "Failed to render prompt section '" + s.id() + "' in template '" + id + "'", e);
          }
        }
      }
      return out;
    }

    @Override
    public String renderText() {
      return String.join(
          "\n\n", renderMessages().stream().map(LlmClient.Message::content).toList());
    }
  }
}
