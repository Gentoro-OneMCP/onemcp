package com.gentoro.onemcp.prompt.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.exception.NotFoundException;
import com.gentoro.onemcp.exception.StateException;
import com.gentoro.onemcp.exception.ValidationException;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Loads prompt YAML templates from the classpath starting at a base directory. Example basePath:
 * "prompts" (will resolve resources like "prompts/summary.yaml").
 */
public class ClasspathPromptRepository implements PromptRepository {
  private final String basePath;
  private final ClassLoader classLoader;

  public ClasspathPromptRepository(String basePath) {
    this(basePath, Thread.currentThread().getContextClassLoader());
  }

  public ClasspathPromptRepository(String basePath, ClassLoader classLoader) {
    this.basePath = normalize(Objects.requireNonNull(basePath, "basePath"));
    this.classLoader =
        Objects.requireNonNullElseGet(
            classLoader, () -> ClasspathPromptRepository.class.getClassLoader());
  }

  @Override
  public PromptTemplate get(String name) {
    try {
      String id = (name.charAt(0) == '/' ? name.substring(1) : name);

      String resource = resolveExisting(id);
      if (resource == null) {
        throw new NotFoundException("Prompt not found on classpath: " + name);
      }

      String yamlContent;
      try (InputStream is = classLoader.getResourceAsStream(resource)) {
        if (is == null) {
          throw new NotFoundException("Prompt resource not found: " + resource);
        }
        byte[] bytes = is.readAllBytes();
        yamlContent = new String(bytes, StandardCharsets.UTF_8);
      }

      JsonNode root = JacksonUtility.getYamlMapper().readTree(yamlContent);
      List<PromptTemplate.PromptSection> sections = new ArrayList<>();

      // Support both new key "sections" and legacy "activations"
      JsonNode arr = root.get("sections");
      if (arr == null) arr = root.get("activations");
      if (arr == null || !arr.isArray()) {
        throw new StateException(
            "Prompt YAML must contain 'sections' (or legacy 'activations') array: " + id);
      }

      for (JsonNode n : arr) {
        String roleStr = n.path("role").asText(null);
        if (roleStr == null) {
          throw new StateException("Missing role for a section in prompt: " + id);
        }
        LlmClient.Role role =
            switch (roleStr.toLowerCase()) {
              case "user" -> LlmClient.Role.USER;
              case "assistant" -> LlmClient.Role.ASSISTANT;
              case "system" -> LlmClient.Role.SYSTEM;
              default -> throw new StateException(
                  "Unknown role '" + roleStr + "' in prompt: " + id);
            };

        String sectionId = n.has("id") ? n.get("id").asText() : n.path("activation").asText(null);
        if (sectionId == null || sectionId.isBlank()) {
          throw new ValidationException("Missing section id (or activation) in prompt: " + id);
        }

        boolean enabled =
            n.has("enabled")
                ? n.get("enabled").asBoolean()
                : n.path("default-state").asBoolean(false);
        String content = n.path("content").asText("");
        if (content.isBlank()) {
          throw new ValidationException(
              "Empty content for section '" + sectionId + "' in prompt: " + id);
        }

        sections.add(new PromptTemplate.PromptSection(role, sectionId, enabled, content));
      }

      return new PebblePromptTemplate(id, sections);
    } catch (Exception e) {
      throw ExceptionUtil.rethrowIfUnchecked(
          e,
          (ex) ->
              new com.gentoro.onemcp.exception.PromptException(
                  "Failed to read prompt file: " + name, ex));
    }
  }

  private String resolveExisting(String id) {
    String yaml = basePath + "/" + id + ".yaml";
    if (classLoader.getResource(yaml) != null) return yaml;
    String yml = basePath + "/" + id + ".yml";
    if (classLoader.getResource(yml) != null) return yml;
    return null;
  }

  private static String normalize(String p) {
    String out = p.trim();
    if (out.startsWith("/")) out = out.substring(1);
    if (out.endsWith("/")) out = out.substring(0, out.length() - 1);
    return out;
  }
}
