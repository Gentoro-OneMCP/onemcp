package com.gentoro.onemcp.compiler;

import com.fasterxml.jackson.core.TreeNode;
import com.gentoro.onemcp.memory.Value;
import com.gentoro.onemcp.memory.ValueStore;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ImportFixer {

  // Common Java and Jackson classes frequently omitted by snippet generators
  private static final Map<String, String> COMMON_IMPORTS =
      Map.ofEntries(
          Map.entry(ValueStore.class.getSimpleName(), ValueStore.class.getName()),
          Map.entry(Value.class.getSimpleName(), Value.class.getName()),
          Map.entry(SnippetBase.class.getSimpleName(), SnippetBase.class.getName()),
          Map.entry(Exception.class.getSimpleName(), Exception.class.getName()),
          Map.entry(RuntimeException.class.getSimpleName(), RuntimeException.class.getName()),
          Map.entry(OkHttpClient.class.getSimpleName(), OkHttpClient.class.getName()),
          Map.entry(Request.class.getSimpleName(), Request.class.getName()),
          Map.entry(Response.class.getSimpleName(), Response.class.getName()),
          Map.entry(RequestBody.class.getSimpleName(), RequestBody.class.getName()),

          // Collections
          Map.entry(Arrays.class.getSimpleName(), Arrays.class.getName()),
          Map.entry("List", "java.util.List"),
          Map.entry("Set", "java.util.Set"),
          Map.entry("Map", "java.util.Map"),
          Map.entry("HashMap", "java.util.HashMap"),
          Map.entry("HashSet", "java.util.HashSet"),
          Map.entry("ArrayList", "java.util.ArrayList"),
          Map.entry("Objects", "java.util.Objects"),
          Map.entry("Collectors", "java.util.stream.Collectors"),
          Map.entry("Stream", "java.util.stream.Stream"),

          // Jackson
          Map.entry(TreeNode.class.getSimpleName(), TreeNode.class.getName()),
          Map.entry("ArrayNode", "com.fasterxml.jackson.databind.node.ArrayNode"),
          Map.entry("JsonNode", "com.fasterxml.jackson.databind.JsonNode"),
          Map.entry("ObjectMapper", "com.fasterxml.jackson.databind.ObjectMapper"),
          Map.entry(
              "JsonProcessingException", "com.fasterxml.jackson.core.JsonProcessingException"));

  /**
   * Scans a Java snippet, detects missing imports, and adds them if known.
   *
   * @param javaCode The Java source code to analyze and fix.
   * @return The Java code with missing imports inserted.
   */
  public static String addMissingImports(String javaCode) {
    if (javaCode == null || javaCode.isBlank()) return javaCode;

    // 1. Extract existing imports
    Set<String> existingImports = new HashSet<>();
    Matcher importMatcher = Pattern.compile("import\\s+([\\w\\.]+);").matcher(javaCode);
    while (importMatcher.find()) {
      existingImports.add(importMatcher.group(1));
    }

    // 2. Extract all class-like tokens used in the code
    Set<String> usedTypes = new HashSet<>();
    Matcher typeMatcher = Pattern.compile("\\b([A-Z][A-Za-z0-9_]*)\\b").matcher(javaCode);
    while (typeMatcher.find()) {
      usedTypes.add(typeMatcher.group(1));
    }

    // 3. Detect missing ones that are known but not yet imported
    List<String> missingImports = new ArrayList<>();
    for (String type : usedTypes) {
      if (COMMON_IMPORTS.containsKey(type)) {
        String fullImport = COMMON_IMPORTS.get(type);
        if (!existingImports.contains(fullImport)) {
          missingImports.add("import " + fullImport + ";");
        }
      }
    }

    if (missingImports.isEmpty()) return javaCode;

    // 4. Insert imports after the package statement (if present)
    StringBuilder fixed = new StringBuilder();
    Matcher pkgMatcher = Pattern.compile("package\\s+[\\w\\.]+;").matcher(javaCode);

    if (pkgMatcher.find()) {
      int pkgEnd = pkgMatcher.end();
      fixed
          .append(javaCode, 0, pkgEnd)
          .append("\n\n")
          .append(String.join("\n", missingImports))
          .append("\n")
          .append(javaCode.substring(pkgEnd));
    } else {
      // No package declaration — prepend imports at the top
      fixed.append(String.join("\n", missingImports)).append("\n\n").append(javaCode);
    }

    return fixed.toString();
  }
}
