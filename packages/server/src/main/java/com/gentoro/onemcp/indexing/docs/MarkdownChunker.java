package com.gentoro.onemcp.indexing.docs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits markdown documents into smaller chunks guided by YAML front matter. Default target size
 * ~500 tokens (very rough approximation using word count).
 */
public class MarkdownChunker {
  /** A chunk of markdown content enriched with optional front-matter metadata. */
  public record Chunk(
      String contentMarkdown, String docPath, List<String> entities, List<String> operations) {}

  /** Supported chunking strategies. */
  public enum Strategy {
    PARAGRAPH,
    HEADING,
    SLIDING_WINDOW
  }

  /** Strategy implementation interface to enable pluggable chunkers. */
  interface MarkdownChunkStrategy {
    List<String> chunk(String body, int windowSizeTokens, int overlapTokens);
  }

  private final int windowSizeTokens;
  private final int overlapTokens;
  private final Strategy strategy;
  private final MarkdownChunkStrategy strategyImpl;

  /** Default constructor uses paragraph strategy with 500 target tokens. */
  public MarkdownChunker() {
    this(Strategy.PARAGRAPH, 500, 0);
  }

  /** Backward-compatible constructor for paragraph strategy with the given target tokens. */
  public MarkdownChunker(int targetTokens) {
    this(Strategy.PARAGRAPH, targetTokens, 0);
  }

  /** Fully configurable constructor. */
  public MarkdownChunker(Strategy strategy, int windowSizeTokens, int overlapTokens) {
    this.strategy = strategy == null ? Strategy.PARAGRAPH : strategy;
    this.windowSizeTokens = Math.max(1, windowSizeTokens);
    this.overlapTokens = Math.max(0, overlapTokens);
    this.strategyImpl =
        switch (this.strategy) {
          case PARAGRAPH -> new ParagraphMarkdownChunkStrategy();
          case HEADING -> new HeadingMarkdownChunkStrategy();
          case SLIDING_WINDOW -> new SlidingWindowMarkdownChunkStrategy();
        };
  }

  public List<Chunk> chunkFile(Path file) {
    try {
      String raw = Files.readString(file, StandardCharsets.UTF_8);
      FrontMatter fm = parseFrontMatter(raw);
      String body = fm.body;
      List<String> entities = fm.entities;
      List<String> ops = fm.operations;

      List<String> contents = strategyImpl.chunk(body, windowSizeTokens, overlapTokens);
      List<Chunk> chunks = new ArrayList<>();
      for (String c : contents) {
        chunks.add(new Chunk(c, file.toString(), entities, ops));
      }
      if (chunks.isEmpty()) chunks.add(new Chunk("", file.toString(), entities, ops));
      return List.copyOf(chunks);
    } catch (Exception e) {
      throw new RuntimeException("Failed to chunk markdown: " + file, e);
    }
  }

  static int estimateTokens(String text) {
    if (text == null || text.isBlank()) return 0;
    // crude approximation: 1 token ~ 0.75 words; we just count words
    String[] words = text.trim().split("\\s+");
    return words.length;
  }

  static List<String> splitParagraphs(String body) {
    if (body == null) return List.of("");
    String[] parts = body.split("\n\n+");
    List<String> list = new ArrayList<>();
    for (String p : parts) {
      if (!p.isBlank()) list.add(p.trim());
    }
    if (list.isEmpty()) list.add(body.trim());
    return list;
  }

  private record FrontMatter(List<String> entities, List<String> operations, String body) {}

  @SuppressWarnings("unchecked")
  private static FrontMatter parseFrontMatter(String markdown) throws Exception {
    List<String> entities = new ArrayList<>();
    List<String> operations = new ArrayList<>();

    String yamlBlock = extractYamlBlock(markdown);
    String body = stripYamlFrontMatter(markdown);
    if (yamlBlock != null) {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      Map<String, Object> yaml = mapper.readValue(yamlBlock, Map.class);
      Object ent = yaml.get("entities");
      if (ent instanceof Collection<?> c) {
        for (Object item : c) {
          if (item instanceof Map<?, ?> m) {
            Object name = m.get("name");
            if (name != null) entities.add(name.toString());
            Object ops = m.get("operations");
            if (ops instanceof Collection<?> oc) {
              for (Object o : oc) operations.add(Objects.toString(o));
            }
          } else {
            entities.add(Objects.toString(item));
          }
        }
      }
    }
    entities = new ArrayList<>(new LinkedHashSet<>(entities));
    operations = new ArrayList<>(new LinkedHashSet<>(operations));
    return new FrontMatter(entities, operations, body);
  }

  private static String extractYamlBlock(String markdown) {
    if (markdown == null) return null;
    Pattern p = Pattern.compile("^---\\s*\n(.*?)\n---\\s*\n", Pattern.DOTALL);
    Matcher m = p.matcher(markdown);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private static String stripYamlFrontMatter(String markdown) {
    if (markdown == null) return null;
    Pattern p = Pattern.compile("^---\\s*\n(.*?)\n---\\s*\n", Pattern.DOTALL);
    Matcher m = p.matcher(markdown);
    if (m.find()) {
      return markdown.substring(m.end());
    }
    return markdown;
  }

  // ---------------- Strategy Implementations ---------------- //

  /** Paragraph batching strategy (previous default behavior). */
  static final class ParagraphMarkdownChunkStrategy implements MarkdownChunkStrategy {
    @Override
    public List<String> chunk(String body, int windowSizeTokens, int overlapTokens) {
      List<String> paragraphs = splitParagraphs(body);
      List<String> out = new ArrayList<>();
      StringBuilder current = new StringBuilder();
      int currentTokens = 0;
      for (String p : paragraphs) {
        int pTokens = estimateTokens(p);
        if (currentTokens + pTokens > windowSizeTokens && current.length() > 0) {
          out.add(current.toString().trim());
          current.setLength(0);
          currentTokens = 0;
        }
        if (pTokens > windowSizeTokens && current.length() == 0) {
          // Extremely long paragraph: hard-split by words to fit window
          List<String> forced =
              new SlidingWindowMarkdownChunkStrategy().chunk(p, windowSizeTokens, 0);
          out.addAll(forced);
          continue;
        }
        current.append(p).append("\n\n");
        currentTokens += pTokens;
      }
      if (current.length() > 0) out.add(current.toString().trim());
      return out;
    }
  }

  /** Heading-based strategy: split by markdown headings, then size-control within sections. */
  static final class HeadingMarkdownChunkStrategy implements MarkdownChunkStrategy {
    private static final Pattern HEADING = Pattern.compile("(?m)^(#{1,6}\\s+.+)$");

    @Override
    public List<String> chunk(String body, int windowSizeTokens, int overlapTokens) {
      if (body == null || body.isBlank()) return List.of("");
      List<Section> sections = splitByHeading(body);
      List<String> out = new ArrayList<>();
      for (Section s : sections) {
        String content = s.toMarkdown();
        int tokens = estimateTokens(content);
        if (tokens <= windowSizeTokens) {
          out.add(content.trim());
        } else {
          // Sub-split large sections by paragraphs while honoring window size
          List<String> paras = splitParagraphs(content);
          StringBuilder current = new StringBuilder();
          int cur = 0;
          for (String p : paras) {
            int pt = estimateTokens(p);
            if (cur + pt > windowSizeTokens && current.length() > 0) {
              out.add(current.toString().trim());
              current.setLength(0);
              cur = 0;
            }
            if (pt > windowSizeTokens && current.length() == 0) {
              out.addAll(new SlidingWindowMarkdownChunkStrategy().chunk(p, windowSizeTokens, 0));
              continue;
            }
            current.append(p).append("\n\n");
            cur += pt;
          }
          if (current.length() > 0) out.add(current.toString().trim());
        }
      }
      return out;
    }

    private record Section(String heading, String content) {
      String toMarkdown() {
        if (heading == null || heading.isBlank()) return content == null ? "" : content;
        return (heading + "\n\n" + (content == null ? "" : content)).trim();
      }
    }

    private static List<Section> splitByHeading(String body) {
      List<Section> sections = new ArrayList<>();
      Matcher m = HEADING.matcher(body);
      int lastIndex = 0;
      String lastHeading = null;
      while (m.find()) {
        int start = m.start();
        if (lastHeading != null) {
          String content = body.substring(lastIndex, start).trim();
          sections.add(new Section(lastHeading, content));
        } else {
          // content before the first heading goes as a headerless section
          String pre = body.substring(0, start).trim();
          if (!pre.isBlank()) sections.add(new Section(null, pre));
        }
        lastHeading = m.group(1).trim();
        lastIndex = m.end();
      }
      // tail
      String tail = body.substring(lastIndex).trim();
      if (lastHeading != null) {
        sections.add(new Section(lastHeading, tail));
      } else if (!tail.isBlank()) {
        sections.add(new Section(null, tail));
      }
      return sections;
    }
  }

  /** Sliding-window strategy over words with configurable overlap. */
  static final class SlidingWindowMarkdownChunkStrategy implements MarkdownChunkStrategy {
    @Override
    public List<String> chunk(String body, int windowSizeTokens, int overlapTokens) {
      if (body == null || body.isBlank()) return List.of("");
      String normalized = body.trim().replaceAll("\n+", "\n");
      String[] words = normalized.split("\\s+");
      int step = Math.max(1, windowSizeTokens - Math.max(0, overlapTokens));
      List<String> out = new ArrayList<>();
      for (int i = 0; i < words.length; i += step) {
        int end = Math.min(words.length, i + windowSizeTokens);
        if (i >= end) break;
        String chunk = String.join(" ", Arrays.copyOfRange(words, i, end)).trim();
        if (!chunk.isBlank()) out.add(chunk);
        if (end == words.length) break;
      }
      return out;
    }
  }
}
