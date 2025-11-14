package com.gentoro.onemcp.messages;

import com.gentoro.onemcp.compiler.ImportFixer;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record StepImplementation(
    @FieldDoc(
            example = "com.gentoro.onemcp.snippets.XYZ",
            description = "Full qualified class name of the implemented Java snippet.",
            required = true)
        String qualifiedClassName,
    @FieldDoc(
            example = "```java ... ```",
            description =
                "The actual Java snippet, use the provided template as an starting point, adding your own logic to it.",
            required = true,
            multiline = true)
        String snippet,
    @FieldDoc(
            example = "Multiline explanation of the produced snippet.",
            description =
                "Complete explanation on what the snippet does, and which values it is expected to produce / change during the execution.",
            required = true,
            multiline = true)
        String explanation) {
  public String qualifiedClassName() {
    String localSnippet = snippet();
    String packageName = extractPackage(localSnippet);
    if (packageName == null || packageName.trim().isBlank()) {
      throw new IllegalArgumentException("No package found in snippet");
    }

    String simpleClassName = extractPublicClassName(localSnippet);
    if (simpleClassName == null || simpleClassName.trim().isBlank()) {
      throw new IllegalArgumentException("No public class found in snippet");
    }

    return "%s.%s".formatted(packageName, simpleClassName);
  }

  public String snippet() {
    String localSnippet = snippet;
    if (localSnippet.startsWith("```java")) {
      localSnippet = StringUtility.extractSnippet(localSnippet, "java");
    }
    localSnippet = ImportFixer.addMissingImports(localSnippet);
    String packageName = extractPackage(localSnippet);
    if (packageName == null || packageName.trim().isBlank()) {
      packageName = "com.gentoro.onemcp.api.snippets";
      localSnippet = "package %s;\n%s".formatted(packageName, localSnippet);
    }
    return localSnippet;
  }

  public static String extractPackage(String javaCode) {
    if (javaCode == null || javaCode.isBlank()) {
      return null;
    }

    // Regular expression to match: package com.example.something;
    Pattern pattern = Pattern.compile("\\bpackage\\s+([a-zA-Z_][\\w\\.]*);");
    Matcher matcher = pattern.matcher(javaCode);

    if (matcher.find()) {
      return matcher.group(1).trim();
    }

    return null;
  }

  public static String extractPublicClassName(String codeSnippet) {
    if (codeSnippet == null) return null;

    // Regex: match "public class <Name>"
    Pattern pattern = Pattern.compile("\\bpublic\\s+class\\s+([A-Za-z_][A-Za-z0-9_]*)");
    Matcher matcher = pattern.matcher(codeSnippet);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  public String toString() {
    return "StepImplementation: {\n"
        + "    explanation: |\n"
        + StringUtility.formatWithIndent(explanation(), 8)
        + "\n"
        + "    qualifiedClassName: |\n"
        + StringUtility.formatWithIndent(qualifiedClassName(), 8)
        + "\n"
        + "    snippet: |\n"
        + StringUtility.formatWithIndent(snippet(), 8)
        + "\n"
        + "}\n";
  }
}
