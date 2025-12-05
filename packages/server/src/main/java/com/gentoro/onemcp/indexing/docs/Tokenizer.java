package com.gentoro.onemcp.indexing.docs;

public class Tokenizer {
  /**
   * Very small heuristic: 1 token ~ 4 characters (approx for many LLMs). You should replace with an
   * actual tokenizer for accurate token counts (e.g. tiktoken for OpenAI).
   */
  public static int estimateTokens(String text) {
    if (text == null || text.isEmpty()) return 0;
    return Math.max(1, text.length() / 4);
  }

  /** Return a substring approximating the last `targetTokens` tokens (approx). */
  public static String lastNTokenApprox(String text, int targetTokens) {
    if (text == null) return "";
    int totalTokens = estimateTokens(text);
    if (targetTokens >= totalTokens) return text;
    double fraction = (double) targetTokens / (double) totalTokens;
    int charLen = text.length();
    int cut = Math.max(0, charLen - (int) Math.ceil(charLen * fraction));
    // move to next whitespace for safety
    if (cut > 0 && cut < charLen) {
      int nextSpace = text.indexOf(' ', cut);
      if (nextSpace > cut) cut = nextSpace;
    }
    return text.substring(cut).trim();
  }
}
