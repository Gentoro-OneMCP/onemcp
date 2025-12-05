package com.gentoro.onemcp.indexing.docs;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.handbook.model.agent.Alias;
import com.gentoro.onemcp.handbook.model.agent.Entity;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.*;
import java.util.stream.Collectors;

/**
 * EntityExtractor: given a chunk and a list of Entities, calls the LLM to extract matches.
 *
 * <p>The prompt is formatted to request JSON output. For production, you should: - use a reliable
 * LLM client that returns valid JSON - use a JSON schema validator or robust parsing
 */
public class EntityExtractor {
  private final OneMcp oneMcp;
  private final List<Entity> entities;

  public EntityExtractor(OneMcp oneMcp, List<Entity> entities) {
    this.oneMcp = oneMcp;
    this.entities = entities == null ? Collections.emptyList() : entities;
  }

  public ChunkEntityExtraction extract(Chunk chunk) {
    if (oneMcp.configuration().getBoolean("graph.chunking.markdown.naiveExtraction", false)) {
      return new ChunkEntityExtraction(chunk.id(), naiveMatch(chunk));
    }
    String prompt = buildPrompt(chunk);
    String response = oneMcp.llmClient().generate(prompt, Collections.emptyList(), false, null);
    try {
      // We expect the response to conform to ChunkEntityExtraction shape.
      // Fallback: if parse fails, do a naive match using entity names and aliases.
      return JacksonUtility.getJsonMapper().readValue(response, ChunkEntityExtraction.class);
    } catch (Exception e) {
      // Fallback: naive extraction
      List<EntityMatch> matches = naiveMatch(chunk);
      return new ChunkEntityExtraction(chunk.id(), matches);
    }
  }

  private String buildPrompt(Chunk chunk) {
    Map<String, Object> minimalEntities = new HashMap<>();
    minimalEntities.put(
        "entities", entities.stream().map(Entity::getName).collect(Collectors.toList()));

    StringBuilder sb = new StringBuilder();
    sb.append("You are an assistant. Given the Entities and a documentation chunk, ");
    sb.append(
        "identify which entities appear in the chunk, whether the chunk 'defines', 'describes', 'uses', or 'references' the entity, ");
    sb.append(
        "give a confidence (0-1) and a short reason. Return strict JSON with the following structure:\n");
    sb.append(
        "{\n  \"chunkId\": \"<id>\",\n  \"matches\": [ {\"entity\":\"<name>\",\"confidence\":0.0,\"reason\":\"...\"} ]\n}\n");
    sb.append("\nEntities:\n");
    for (Entity e : entities) {
      sb.append("- ").append(e.getName());
      if (e.getAliases() != null && !e.getAliases().isEmpty()) {
        sb.append(" (aliases: ")
            .append(
                e.getAliases().stream()
                    .map(Alias::getTerms)
                    .flatMap(Collection::stream)
                    .collect(Collectors.joining(", ")))
            .append(")");
      }
      if (e.getDescription() != null) {
        sb.append(": ").append(e.getDescription());
      }
      sb.append("\n");
    }
    sb.append("\nChunkId: ").append(chunk.id()).append("\n");
    sb.append("ChunkContent:\n---\n").append(chunk.content()).append("\n---\n");
    sb.append("Return JSON only.");
    return sb.toString();
  }

  // Naive fallback: match names and aliases, produce guesses
  private List<EntityMatch> naiveMatch(Chunk chunk) {
    String lower = chunk.content().toLowerCase(Locale.ROOT);
    List<EntityMatch> out = new ArrayList<>();
    for (Entity e : entities) {
      boolean found = false;
      if (e.getName() != null && lower.contains(e.getName().toLowerCase(Locale.ROOT))) {
        found = true;
      }
      if (!found && e.getAliases() != null) {
        for (String a :
            e.getAliases().stream().map(Alias::getTerms).flatMap(Collection::stream).toList()) {
          if (a != null && lower.contains(a.toLowerCase(Locale.ROOT))) {
            found = true;
            break;
          }
        }
      }
      if (found) {
        out.add(new EntityMatch(e.getName(), 0.9, "Matched by exact name or alias (fallback)"));
      }
    }
    return out;
  }
}
