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
    return splitBlockRecursively(text, sectionPath, fileName, 0);
  }

  // Recursively split large blocks into chunks that fit token limits.
  // depth parameter prevents infinite recursion
  private List<Chunk> splitBlockRecursively(
      String text, List<String> sectionPath, String fileName, int depth) {
    // Safety: prevent infinite recursion (max 10 levels)
    if (depth > 10) {
      // Force split by character count as last resort
      return forceSplitBySize(text, sectionPath, fileName);
    }

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
    // Use manual splitting to avoid regex stack overflow on very long texts
    List<String> paragraphs = splitByParagraphs(text);
    if (paragraphs.size() > 1) {
      StringBuilder collector = new StringBuilder();
      for (String p : paragraphs) {
        if (collector.length() == 0) {
          collector.append(p.trim());
        } else {
          // try append paragraph; if becomes too large, flush
          String candidate = collector.toString() + "\n\n" + p.trim();
          if (Tokenizer.estimateTokens(candidate) > maxTokens) {
            result.addAll(splitBlockRecursively(collector.toString(), sectionPath, fileName, depth + 1));
            collector = new StringBuilder(p.trim());
          } else {
            collector = new StringBuilder(candidate);
          }
        }
      }
      if (collector.length() > 0) {
        result.addAll(splitBlockRecursively(collector.toString(), sectionPath, fileName, depth + 1));
      }
      return result;
    }

    // fallback: split by sentence boundaries
    // Use a more robust approach to avoid regex stack overflow on very long texts
    List<String> sentences = splitBySentences(text);
    
    // Safety check: if sentence splitting didn't actually split anything, force split
    if (sentences.size() <= 1) {
      return forceSplitBySize(text, sectionPath, fileName);
    }
    
    StringBuilder coll = new StringBuilder();
    for (String s : sentences) {
      if (coll.length() == 0) {
        coll.append(s.trim());
      } else {
        String candidate = coll.toString() + " " + s.trim();
        if (Tokenizer.estimateTokens(candidate) > maxTokens) {
          // flush
          result.addAll(splitBlockRecursively(coll.toString(), sectionPath, fileName, depth + 1));
          coll = new StringBuilder(s.trim());
        } else {
          coll = new StringBuilder(candidate);
        }
      }
    }
    if (coll.length() > 0) {
      // Safety: check if we're making progress (text should be smaller)
      int collTokens = Tokenizer.estimateTokens(coll.toString());
      if (collTokens >= tokens) {
        // No progress made, force split to avoid infinite recursion
        return forceSplitBySize(coll.toString(), sectionPath, fileName);
      }
      result.addAll(splitBlockRecursively(coll.toString(), sectionPath, fileName, depth + 1));
    }

    return result;
  }

  /**
   * Force split text by character count as last resort to prevent infinite recursion.
   * Splits text into chunks of approximately maxTokens size.
   */
  private List<Chunk> forceSplitBySize(String text, List<String> sectionPath, String fileName) {
    List<Chunk> result = new ArrayList<>();
    if (text == null || text.isEmpty()) {
      return result;
    }
    
    // Estimate characters per token (roughly 4 chars per token)
    int charsPerToken = 4;
    int maxChars = maxTokens * charsPerToken;
    
    int len = text.length();
    int start = 0;
    
    while (start < len) {
      int end = Math.min(start + maxChars, len);
      
      // Try to break at word boundary
      if (end < len) {
        // Look backwards for whitespace
        int lastSpace = end;
        for (int i = end - 1; i > start && i > end - 100; i--) {
          if (Character.isWhitespace(text.charAt(i))) {
            lastSpace = i;
            break;
          }
        }
        end = lastSpace;
      }
      
      String chunkText = text.substring(start, end).trim();
      if (!chunkText.isEmpty()) {
        result.add(
            new Chunk(
                UUID.randomUUID().toString(), fileName, joinPath(sectionPath), chunkText));
      }
      
      // Skip whitespace at the start of next chunk
      start = end;
      while (start < len && Character.isWhitespace(text.charAt(start))) {
        start++;
      }
    }
    
    return result;
  }

  private String joinPath(List<String> path) {
    if (path == null || path.isEmpty()) return "";
    return String.join(" > ", path);
  }

  /**
   * Splits text by paragraphs (double newlines) without using regex that can cause stack overflow.
   * Uses a simple character-by-character approach for better reliability on very long texts.
   * 
   * <p>This method avoids the regex pattern {@code \n\\s*\n} which can cause
   * {@code PatternSyntaxException: Stack overflow} on very long texts.
   * 
   * <p>A paragraph boundary is defined as: newline, followed by optional whitespace, followed by another newline.
   */
  private List<String> splitByParagraphs(String text) {
    List<String> paragraphs = new ArrayList<>();
    if (text == null || text.isEmpty()) {
      return paragraphs;
    }

    StringBuilder current = new StringBuilder();
    int len = text.length();
    int i = 0;
    
    while (i < len) {
      char c = text.charAt(i);
      
      if (c == '\n') {
        // Check if this newline is followed by optional whitespace and another newline
        int j = i + 1;
        // Skip whitespace (but not newlines)
        while (j < len && Character.isWhitespace(text.charAt(j)) && text.charAt(j) != '\n') {
          j++;
        }
        // Check if we found another newline
        if (j < len && text.charAt(j) == '\n') {
          // Found paragraph boundary (newline + whitespace + newline)
          String para = current.toString().trim();
          if (!para.isEmpty()) {
            paragraphs.add(para);
          }
          current = new StringBuilder();
          // Skip to after the second newline
          // Skip any trailing whitespace (but not newlines, as they might be part of next boundary)
          i = j + 1; // Move past the second newline
          // Only skip non-newline whitespace to avoid missing the next paragraph boundary
          while (i < len && Character.isWhitespace(text.charAt(i)) && text.charAt(i) != '\n') {
            i++;
          }
          // Continue processing from current position (don't increment here, while loop will handle it)
          continue;
        } else {
          // Not a paragraph boundary, just a regular newline
          current.append(c);
          i++;
        }
      } else {
        current.append(c);
        i++;
      }
    }
    
    // Add remaining text as final paragraph if any
    String remaining = current.toString().trim();
    if (!remaining.isEmpty()) {
      paragraphs.add(remaining);
    }
    
    // If no paragraphs were found, return the whole text as one paragraph
    if (paragraphs.isEmpty()) {
      paragraphs.add(text.trim());
    }
    
    return paragraphs;
  }

  /**
   * Splits text by sentence boundaries without using complex regex that can cause stack overflow.
   * Uses a simple character-by-character approach for better reliability on very long texts.
   * 
   * <p>This method avoids the regex pattern {@code (?<=[.!?])\\s+} which can cause
   * {@code PatternSyntaxException: Stack overflow} on very long texts with many sentence boundaries.
   * 
   * <p>Note: This is a simple heuristic that splits on punctuation followed by whitespace.
   * It may occasionally split on abbreviations or decimal numbers, but this is acceptable
   * for chunking purposes as the chunks will still be semantically meaningful.
   */
  private List<String> splitBySentences(String text) {
    List<String> sentences = new ArrayList<>();
    if (text == null || text.isEmpty()) {
      return sentences;
    }

    StringBuilder current = new StringBuilder();
    int len = text.length();
    
    for (int i = 0; i < len; i++) {
      char c = text.charAt(i);
      current.append(c);
      
      // Check for sentence-ending punctuation followed by whitespace
      if ((c == '.' || c == '!' || c == '?') && i < len - 1) {
        char next = text.charAt(i + 1);
        if (Character.isWhitespace(next)) {
          // Found sentence boundary - add current sentence and reset
          String sentence = current.toString().trim();
          if (!sentence.isEmpty()) {
            sentences.add(sentence);
          }
          current = new StringBuilder();
          // Skip all following whitespace
          while (i + 1 < len && Character.isWhitespace(text.charAt(i + 1))) {
            i++;
          }
        }
      }
    }
    
    // Add remaining text as final sentence if any
    String remaining = current.toString().trim();
    if (!remaining.isEmpty()) {
      sentences.add(remaining);
    }
    
    // If no sentences were found (e.g., no punctuation), return the whole text as one sentence
    if (sentences.isEmpty()) {
      sentences.add(text.trim());
    }
    
    return sentences;
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
