package com.gentoro.onemcp.indexing.docs;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MarkdownChunkerTest {

  @Test
  @DisplayName(
      "Paragraph strategy groups paragraphs up to window size and hard-splits long paragraphs")
  void paragraphStrategy() throws Exception {
    String md =
        """
        Para1 word1 word2 word3\n\n
        Para2 word1 word2 word3 word4 word5 word6\n\n
        %s
        """
            .formatted("long ".repeat(200));
    Path tmp = Files.createTempFile("md_para_", ".md");
    Files.writeString(tmp, md);

    MarkdownChunker c = new MarkdownChunker(MarkdownChunker.Strategy.PARAGRAPH, 20, 0);
    List<MarkdownChunker.Chunk> chunks = c.chunkFile(tmp);
    assertTrue(chunks.size() >= 3, "should produce multiple chunks including forced split");
    assertTrue(chunks.getFirst().contentMarkdown().contains("Para1"));
    assertNotNull(chunks.getFirst().entities());
    assertNotNull(chunks.getFirst().operations());
  }

  @Test
  @DisplayName("Heading strategy splits by headings and size-controls large sections")
  void headingStrategy() throws Exception {
    String md =
        """
        Intro text before headings.

        # H1 Section
        H1 content line 1

        ## H2 Section
        H2 content line 1 H2 content line 2

        # Another H1
        %s
        """
            .formatted("w ".repeat(150));
    Path tmp = Files.createTempFile("md_head_", ".md");
    Files.writeString(tmp, md);

    MarkdownChunker c = new MarkdownChunker(MarkdownChunker.Strategy.HEADING, 40, 0);
    List<MarkdownChunker.Chunk> chunks = c.chunkFile(tmp);
    assertTrue(chunks.size() >= 3);
    assertTrue(chunks.stream().anyMatch(ch -> ch.contentMarkdown().startsWith("# H1 Section")));
  }

  @Test
  @DisplayName("Sliding window produces overlapping windows and handles edge cases")
  void slidingWindowStrategy() throws Exception {
    String body = String.join(" ", java.util.Collections.nCopies(60, "w"));
    String md = body; // no front matter
    Path tmp = Files.createTempFile("md_sw_", ".md");
    Files.writeString(tmp, md);

    MarkdownChunker c = new MarkdownChunker(MarkdownChunker.Strategy.SLIDING_WINDOW, 20, 5);
    List<MarkdownChunker.Chunk> chunks = c.chunkFile(tmp);
    assertEquals(4, chunks.size(), "60 tokens with size 20 step 15 -> 4 chunks");
    assertTrue(chunks.get(0).contentMarkdown().split("\\s+").length <= 20);

    // overlap >= window -> step 1
    c = new MarkdownChunker(MarkdownChunker.Strategy.SLIDING_WINDOW, 10, 20);
    chunks = c.chunkFile(tmp);
    assertEquals(51, chunks.size());
  }

  @Test
  @DisplayName("Front matter YAML extracts entities and operations and is removed from content")
  void frontMatterExtraction() throws Exception {
    String md =
        """
        ---
        entities:
          - name: Order
            operations: [Retrieve, Create]
          - User
        ---
        # Title
        Body text here.
        """;
    Path tmp = Files.createTempFile("md_fm_", ".md");
    Files.writeString(tmp, md);

    MarkdownChunker c = new MarkdownChunker(MarkdownChunker.Strategy.HEADING, 100, 0);
    List<MarkdownChunker.Chunk> chunks = c.chunkFile(tmp);
    assertEquals(List.of("Order", "User"), chunks.getFirst().entities());
    assertEquals(List.of("Retrieve", "Create"), chunks.getFirst().operations());
    assertFalse(chunks.getFirst().contentMarkdown().contains("---"));
  }
}
