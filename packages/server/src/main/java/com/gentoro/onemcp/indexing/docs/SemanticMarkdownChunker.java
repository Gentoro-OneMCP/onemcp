package com.gentoro.onemcp.indexing.docs;

import com.vladsch.flexmark.ast.*;
import com.vladsch.flexmark.ext.tables.TableBlock;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Node;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Semantic markdown chunker: - Parses markdown with flexmark - Groups content by heading sections
 * (keeps section path) - Protects code blocks, tables, lists (keeps them whole) - Recursively
 * splits by paragraphs, then sentences - Applies overlap (token-based) if requested
 */
public class SemanticMarkdownChunker {

  private final int minTokens;
  private final int maxTokens;
  private final int overlapTokens;
  private final Parser parser = Parser.builder().build();

  public SemanticMarkdownChunker(int minTokens, int maxTokens, int overlapTokens) {
    if (minTokens <= 0 || maxTokens <= 0 || maxTokens < minTokens)
      throw new IllegalArgumentException("Invalid token sizes");
    this.minTokens = minTokens;
    this.maxTokens = maxTokens;
    this.overlapTokens = Math.max(0, overlapTokens);
  }

  public List<Chunk> chunkFile(Path file) throws IOException {
    String md = Files.readString(file);
    Node root = parser.parse(md);
    List<SectionBlock> sections = extractSections(root);
    List<Chunk> chunks = new ArrayList<>();
    for (SectionBlock s : sections) {
      chunks.addAll(
          splitBlockRecursively(s.content(), s.sectionPath(), file.getFileName().toString()));
    }
    // apply overlap
    if (overlapTokens > 0 && !chunks.isEmpty()) {
      chunks = applyOverlap(chunks, overlapTokens);
    }
    return chunks;
  }

  // Extract top-to-bottom sections by accumulating heading path.
  private List<SectionBlock> extractSections(Node root) {
    List<SectionBlock> out = new ArrayList<>();
    Deque<String> headingStack = new ArrayDeque<>();
    StringBuilder current = new StringBuilder();
    List<String> currentPath = new ArrayList<>();

    Node node = root.getFirstChild();
    while (node != null) {
      if (node instanceof Heading h) {
        // When encountering a heading, flush previous collected block (if any)
        if (current.length() > 0) {
          out.add(new SectionBlock(current.toString().trim(), new ArrayList<>(currentPath)));
          current = new StringBuilder();
        }
        // maintain heading stack/path
        int level = h.getLevel();
        // collapse stack to appropriate level
        while (headingStack.size() >= level) {
          headingStack.removeLast();
        }
        headingStack.addLast(h.getText().toString());
        currentPath = new ArrayList<>(headingStack);
      } else {
        // protected blocks: code, fenced code, bullets, lists, tables
        if (node instanceof FencedCodeBlock
            || node instanceof CodeBlock
            || node instanceof BulletList
            || node instanceof OrderedList
            || node instanceof TableBlock
            || node instanceof HtmlBlock) {
          // flush any current text as its own block first
          if (current.length() > 0) {
            out.add(new SectionBlock(current.toString().trim(), new ArrayList<>(currentPath)));
            current = new StringBuilder();
          }
          out.add(
              new SectionBlock(node.getChars().toString().trim(), new ArrayList<>(currentPath)));
        } else {
          // accumulate text node
          String t = node.getChars().toString();
          if (!t.isBlank()) {
            current.append(t).append("\n\n");
          }
        }
      }
      node = node.getNext();
    }

    if (current.length() > 0) {
      out.add(new SectionBlock(current.toString().trim(), new ArrayList<>(currentPath)));
    }
    return out;
  }

  // Recursively split large blocks into chunks that fit token limits.
  private List<Chunk> splitBlockRecursively(
      String text, List<String> sectionPath, String fileName) {
    List<Chunk> result = new ArrayList<>();
    int tokens = Tokenizer.estimateTokens(text);
    if (tokens <= maxTokens && tokens >= minTokens) {
      result.add(
          new Chunk(UUID.randomUUID().toString(), fileName, joinPath(sectionPath), text.trim()));
      return result;
    }

    if (tokens < minTokens) {
      // If block too small, still emit (allow downstream merging)
      result.add(
          new Chunk(UUID.randomUUID().toString(), fileName, joinPath(sectionPath), text.trim()));
      return result;
    }

    // try paragraph split
    String[] paragraphs = text.split("\n\\s*\n");
    if (paragraphs.length > 1) {
      StringBuilder collector = new StringBuilder();
      for (String p : paragraphs) {
        if (collector.length() == 0) {
          collector.append(p.trim());
        } else {
          // try append paragraph; if becomes too large, flush
          String candidate = collector.toString() + "\n\n" + p.trim();
          if (Tokenizer.estimateTokens(candidate) > maxTokens) {
            result.addAll(splitBlockRecursively(collector.toString(), sectionPath, fileName));
            collector = new StringBuilder(p.trim());
          } else {
            collector = new StringBuilder(candidate);
          }
        }
      }
      if (collector.length() > 0) {
        result.addAll(splitBlockRecursively(collector.toString(), sectionPath, fileName));
      }
      return result;
    }

    // fallback: split by sentence boundaries
    String[] sentences = text.split("(?<=[.!?])\\s+");
    StringBuilder coll = new StringBuilder();
    for (String s : sentences) {
      if (coll.length() == 0) {
        coll.append(s.trim());
      } else {
        String candidate = coll.toString() + " " + s.trim();
        if (Tokenizer.estimateTokens(candidate) > maxTokens) {
          // flush
          result.addAll(splitBlockRecursively(coll.toString(), sectionPath, fileName));
          coll = new StringBuilder(s.trim());
        } else {
          coll = new StringBuilder(candidate);
        }
      }
    }
    if (coll.length() > 0) {
      result.addAll(splitBlockRecursively(coll.toString(), sectionPath, fileName));
    }

    return result;
  }

  private String joinPath(List<String> path) {
    if (path == null || path.isEmpty()) return "";
    return String.join(" > ", path);
  }

  // Prepend the last `overlapTokens` tokens approximated from previous chunk to the next chunk.
  private List<Chunk> applyOverlap(List<Chunk> chunks, int overlapTokens) {
    List<Chunk> out = new ArrayList<>();
    Chunk prev = null;
    for (Chunk c : chunks) {
      String content = c.content();
      if (prev != null) {
        String tail = Tokenizer.lastNTokenApprox(prev.content(), overlapTokens);
        // prepend tail only if not already present to avoid duplication
        if (!content.startsWith(tail)) {
          content = tail + "\n\n" + content;
        }
      }
      out.add(new Chunk(c.id(), c.fileName(), c.sectionPath(), content));
      prev = c;
    }
    return out;
  }

  private static record SectionBlock(String content, List<String> sectionPath) {}
}
