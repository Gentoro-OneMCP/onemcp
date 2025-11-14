package com.gentoro.onemcp.messages;

import java.lang.reflect.*;
import java.util.*;

public class YamlTemplateGenerator {

  public static String generateYaml(Class<?> recordClass) {
    StringBuilder sb = new StringBuilder();
    sb.append("```yaml\n");
    sb.append("message:\n");
    sb.append("    # Constant value of `")
        .append(recordClass.getSimpleName())
        .append("`, defining the message data type.\n");
    sb.append("    type: ").append(recordClass.getSimpleName()).append("\n");
    sb.append("    data:\n");
    processRecord(recordClass, sb, "        ");
    sb.append("```\n\n");

    sb.append(
        """
        ---

        ## ⚙️ YAML Formatting Rules

        You must respond **only** with valid, well-formatted YAML. \s

        - Use **4 spaces per indentation level** consistently. \s
        - **Do not include markdown code fences**, explanations, or comments in your output. \s
        - Use **YAML block scalars (`|` or `>`)** for multi-line strings such as descriptions, narratives, or code snippets. \s
        - Maintain **4-space indentation inside multi-line blocks** relative to the parent key. \s
        - All other fields should remain **single-line**, unless semantically multi-line. \s
        - The YAML must be **syntactically valid (YAML 1.2 compliant)** and parse correctly as-is. \s
        - When using block scalars (`|` or `>`), wrap lines to approximately **80 characters** for readability. \s


        """);

    return sb.toString();
  }

  private static void processRecord(Class<?> recordClass, StringBuilder sb, String indent) {
    for (RecordComponent component : recordClass.getRecordComponents()) {
      String name = component.getName();
      Type genericType = component.getGenericType();
      Class<?> rawType = component.getType();

      FieldDoc doc = component.getAnnotation(FieldDoc.class);

      String typeHint = rawType.getSimpleName();
      String example = doc != null ? doc.example() : "";
      boolean required = doc != null && doc.required();
      boolean multiline = doc != null && doc.multiline();
      String pattern = doc != null ? doc.pattern() : "";
      String description = doc != null ? doc.description() : "";

      addComments(sb, indent, description, required, pattern);

      // handle List<T> generics
      if (List.class.isAssignableFrom(rawType)) {
        sb.append(indent).append(name).append(":\n");

        Type elementType = null;
        if (genericType instanceof ParameterizedType paramType) {
          elementType = paramType.getActualTypeArguments()[0];
        }

        if (elementType instanceof Class<?> elementClass) {
          if (elementClass.isRecord()) {
            // list of records
            sb.append(indent).append("    - \n");
            processRecord(elementClass, sb, indent + "        ");
          }
        } else {
          sb.append(indent).append("    - \n");
        }

        sb.append("\n");
        continue;
      }

      // handle nested records
      if (rawType.isRecord()) {
        sb.append(indent).append(name).append(":\n");
        processRecord(rawType, sb, indent + "    ");
        sb.append("\n");
        continue;
      }

      // primitive/simple field
      sb.append(indent).append(name).append(": ");
      if (multiline) {
        sb.append("|\n");
        sb.append(indent).append(indent).append("...\n");
      }
      sb.append("\n");
    }
  }

  private static void addComments(
      StringBuilder sb, String indent, String description, boolean required, String pattern) {
    Arrays.stream(description.split("\n")).map(l -> indent + "# " + l + "\n").forEach(sb::append);
  }

  public static void main(String[] args) {
    System.out.println(generateYaml(StepImplementation.class));
  }
}
