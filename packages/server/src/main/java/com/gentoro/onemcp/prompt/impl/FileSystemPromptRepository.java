package com.gentoro.onemcp.prompt.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class FileSystemPromptRepository implements PromptRepository {
  private final Path path;

  public FileSystemPromptRepository(Path path) throws IOException {
    this.path = path;
  }

  @Override
  public PromptTemplate get(String name) {
    try {
      String id = (name.charAt(0) == '/' ? name.substring(1) : name);
      Path yamlPath = resolveExisting(id);
      if (yamlPath == null) {
        throw new IllegalArgumentException("Prompt not found: " + name);
      }
      String yamlContent = Files.readString(yamlPath);

      JsonNode root = JacksonUtility.getYamlMapper().readTree(yamlContent);
      List<PromptTemplate.PromptSection> sections = new ArrayList<>();

      // Support both new key "sections" and legacy "activations"
      JsonNode arr = root.get("sections");
      if (arr == null) arr = root.get("activations");
      if (arr == null || !arr.isArray()) {
        throw new IllegalArgumentException(
            "Prompt YAML must contain 'sections' (or legacy 'activations') array: " + id);
      }

      for (JsonNode n : arr) {
        String roleStr = n.path("role").asText(null);
        if (roleStr == null) {
          throw new IllegalArgumentException("Missing role for a section in prompt: " + id);
        }
        LlmClient.Role role =
            switch (roleStr.toLowerCase()) {
              case "user" -> LlmClient.Role.USER;
              case "assistant" -> LlmClient.Role.ASSISTANT;
              case "system" -> LlmClient.Role.SYSTEM;
              default -> throw new IllegalArgumentException(
                  "Unknown role '" + roleStr + "' in prompt: " + id);
            };

        String sectionId = n.has("id") ? n.get("id").asText() : n.path("activation").asText(null);
        if (sectionId == null || sectionId.isBlank()) {
          throw new IllegalArgumentException("Missing section id (or activation) in prompt: " + id);
        }

        boolean enabled =
            n.has("enabled")
                ? n.get("enabled").asBoolean()
                : n.path("default-state").asBoolean(false);
        String content = n.path("content").asText("");
        if (content.isBlank()) {
          throw new IllegalArgumentException(
              "Empty content for section '" + sectionId + "' in prompt: " + id);
        }

        sections.add(new PromptTemplate.PromptSection(role, sectionId, enabled, content));
      }

      return new PebblePromptTemplate(id, sections);
    } catch (Exception e) {
      throw new com.gentoro.onemcp.exception.PromptException(
          "Failed to read prompt file: " + name, e);
    }
  }

  private Path resolveExisting(String id) {
    Path pYaml = path.resolve(id + ".yaml");
    if (Files.exists(pYaml)) return pYaml;
    Path pYml = path.resolve(id + ".yml");
    if (Files.exists(pYml)) return pYml;
    return null;
  }
}
