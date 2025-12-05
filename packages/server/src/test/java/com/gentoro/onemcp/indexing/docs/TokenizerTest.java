package com.gentoro.onemcp.indexing.docs;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TokenizerTest {

  @Test
  @DisplayName("estimateTokens handles null/empty and basic lengths")
  void estimateTokensBasic() {
    assertEquals(0, Tokenizer.estimateTokens(null));
    assertEquals(0, Tokenizer.estimateTokens(""));
    assertEquals(1, Tokenizer.estimateTokens("a"));
    assertEquals(1, Tokenizer.estimateTokens("abcd")); // 4/4 = 1
    assertEquals(2, Tokenizer.estimateTokens("abcdefgh")); // 8/4 = 2
    assertEquals(3, Tokenizer.estimateTokens("abcdefghijkl")); // 12/4 = 3
  }

  @Test
  @DisplayName("lastNTokenApprox returns full string when target >= total tokens")
  void lastNTokensReturnsFullWhenLarge() {
    String text = "The quick brown fox jumps over the lazy dog";
    int total = Tokenizer.estimateTokens(text);
    assertEquals(text, Tokenizer.lastNTokenApprox(text, total));
    assertEquals(text, Tokenizer.lastNTokenApprox(text, total + 10));
  }

  @Test
  @DisplayName("lastNTokenApprox returns suffix at a whitespace boundary")
  void lastNTokensSuffixBoundary() {
    String text = "one two three four five six seven eight nine ten";
    int total = Tokenizer.estimateTokens(text);
    String suffix = Tokenizer.lastNTokenApprox(text, Math.max(1, total / 2));

    assertTrue(text.endsWith(suffix), "Suffix should be at the end of original text");
    assertFalse(suffix.isEmpty());
    assertTrue(suffix.length() < text.length());

    // Ensure we don't start mid-word (best-effort, since heuristic picks next space)
    int idx = text.length() - suffix.length();
    if (idx > 0) {
      // Either previous char is a space or suffix begins at start
      assertEquals(' ', text.charAt(idx - 1));
    }
  }
}
