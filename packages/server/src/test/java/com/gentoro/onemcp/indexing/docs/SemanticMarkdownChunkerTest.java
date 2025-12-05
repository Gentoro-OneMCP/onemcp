package com.gentoro.onemcp.indexing.docs;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SemanticMarkdownChunkerTest {

  private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("md_chunker_test_");
  }

  @AfterEach
  void tearDown() throws Exception {
    // Best-effort cleanup
    try (var s = Files.walk(tempDir)) {
      s.sorted((a, b) -> b.compareTo(a))
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (Exception ignored) {
                }
              });
    }
  }

  @Test
  @DisplayName("Builds section path and preserves protected blocks as their own chunks")
  void sectionPathsAndProtectedBlocks() throws Exception {
    String md =
        "# A\n\n"
            + "Plain text under A.\n\n"
            + "```\ncode block\nline2\n```\n\n"
            + "- item1\n- item2\n";

    Path f = tempDir.resolve("doc.md");
    Files.writeString(f, md);

    SemanticMarkdownChunker chunker = new SemanticMarkdownChunker(1, 10_000, 0);
    List<Chunk> chunks = chunker.chunkFile(f);

    // Expect three chunks: plain text, code fence, and list block
    assertEquals(3, chunks.size(), "Expected plain, code, and list chunks");
    for (Chunk c : chunks) {
      assertEquals("A", c.sectionPath());
      assertEquals("doc.md", c.fileName());
    }
    assertTrue(chunks.get(0).content().contains("Plain text"));
    assertTrue(chunks.get(1).content().contains("code block"));
    assertTrue(chunks.get(1).content().contains("```"), "Fenced code should be preserved");
    assertTrue(chunks.get(2).content().contains("- item1"));
  }

  @Test
  @DisplayName("Splits by paragraphs when exceeding max tokens; sentences as fallback")
  void paragraphAndSentenceSplitting() throws Exception {
    String md =
        "# Sec\n\n"
            + "Para one is short.\n\n"
            + "Para two is a bit longer and should likely cause splitting depending on token limits. "
            + "It has multiple sentences. Another one here. And one more.";
    Path f = tempDir.resolve("split.md");
    Files.writeString(f, md);

    // Small enough to force splitting, but large enough that the smallest sentence fits
    SemanticMarkdownChunker chunker = new SemanticMarkdownChunker(1, 25, 0);
    List<Chunk> chunks = chunker.chunkFile(f);

    assertTrue(chunks.size() >= 2, "Should produce multiple chunks");
    for (Chunk c : chunks) {
      assertTrue(
          Tokenizer.estimateTokens(c.content()) <= 25, "Chunk should respect max token constraint");
    }
  }

  @Test
  @DisplayName("Applies token overlap between consecutive chunks without duplication")
  void overlapApplied() throws Exception {
    String para =
        "This paragraph is intentionally long to force splitting into multiple chunks. "
            + "It contains several sentences. Splitting should occur here. Another sentence follows. End.";
    String md = "# Overlap\n\n" + para + "\n\n" + para;
    Path f = tempDir.resolve("overlap.md");
    Files.writeString(f, md);

    SemanticMarkdownChunker base = new SemanticMarkdownChunker(1, 40, 0);
    List<Chunk> baseChunks = base.chunkFile(f);
    assertTrue(baseChunks.size() > 1, "Base case should create multiple chunks");

    SemanticMarkdownChunker withOverlap = new SemanticMarkdownChunker(1, 40, 5);
    List<Chunk> overlapped = withOverlap.chunkFile(f);
    assertEquals(baseChunks.size(), overlapped.size(), "Overlap should not change chunk count");

    for (int i = 1; i < overlapped.size(); i++) {
      String tail = Tokenizer.lastNTokenApprox(baseChunks.get(i - 1).content(), 5);
      assertTrue(
          overlapped.get(i).content().startsWith(tail),
          "Next chunk should start with tail from previous chunk");
    }
  }
}
