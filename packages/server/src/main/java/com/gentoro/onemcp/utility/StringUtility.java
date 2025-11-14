package com.gentoro.onemcp.utility;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StringUtility {

  public static String sanitize(String input) {
    return input
        .replaceAll("[^a-zA-Z0-9]", "_") // Replace invalid characters with underscores
        .replaceAll("_+", "_"); // Collapse multiple underscores into one
  }

  public static String formatWithIndent(String input, int indent) {
    return formatWithIndent(input, indent, 1000);
  }

  public static String formatWithIndent(String input, int indent, int limit) {
    if (input == null) return "";
    if (indent < 0) indent = 0;

    // Build indentation string
    String spaces = " ".repeat(indent);

    // Normalize all <br> variants to newline
    String formatted =
        input
            .replaceAll("(?i)<br\\s*/?>", "\n") // handle <br>, <br/>, <BR>, etc.
            .replaceAll("\\r\\n?", "\n") // normalize CRLF or CR to LF
            .trim();

    // Split into lines and rejoin with indentation applied
    String[] lines = formatted.split("\n");
    if (limit > -1 && lines.length > limit) {
      return Arrays.stream(formatted.split("\n"))
              .map(line -> spaces + line)
              .limit(limit)
              .collect(Collectors.joining("\n"))
          + " ...";

    } else {
      return Arrays.stream(formatted.split("\n"))
          .map(line -> spaces + line)
          .collect(Collectors.joining("\n"));
    }
  }

  public static String extractSnippet(String text, String type) {
    if (text == null || text.isEmpty()) {
      return null;
    }

    // Regex to match JSON inside markdown or plain text
    String regex = "(?s)(?:```%s\\s*)(.+)(?:\\s*```)".formatted(type);
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(text);

    if (matcher.find()) {
      return matcher.group(1).trim();
    }

    return null;
  }
}
