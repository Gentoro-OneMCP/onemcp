package com.gentoro.onemcp.openapi;

import com.gentoro.onemcp.exception.ValidationException;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.*;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponses;
import java.util.*;

/**
 * MarkdownGenerator ------------------ Generates Markdown + JSON5 documentation for an OpenAPI
 * specification. Designed for LLM ingestion.
 */
public class MarkdownGenerator {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(MarkdownGenerator.class);

  // ==============================================================
  // Entry point
  // ==============================================================

  public static List<Map<String, Object>> generate(OpenAPI openAPI) {
    if (openAPI.getPaths() == null || openAPI.getPaths().isEmpty()) {
      throw new ValidationException("No paths defined in OpenAPI specification");
    }

    log.trace("Generating Markdown API: {}", openAPI.getInfo().getTitle());
    List<Map<String, Object>> result = new ArrayList<>();

    openAPI
        .getPaths()
        .forEach(
            (path, pathItem) -> {
              if (pathItem == null) return;
              for (Map.Entry<PathItem.HttpMethod, Operation> e :
                  pathItem.readOperationsMap().entrySet()) {
                result.add(
                    generateEndpointMarkdown(openAPI, path, e.getKey().name(), e.getValue()));
              }
            });

    return result;
  }

  // ==============================================================
  // Endpoint renderer
  // ==============================================================

  private static Map<String, Object> generateEndpointMarkdown(
      OpenAPI openAPI, String path, String method, Operation op) {
    log.trace("Generating Markdown for path: {}/{}", method, path);
    StringBuilder md = new StringBuilder();
    md.append("```md\n");
    if (op.getSummary() != null && !op.getSummary().isEmpty())
      md.append((op.getSummary())).append("\n\n");
    if (op.getDescription() != null && !op.getDescription().isEmpty())
      md.append((op.getDescription())).append("\n\n");
    md.append("```\n\n");
    // Parameters (query/path/header/cookie)
    List<Parameter> params =
        Optional.ofNullable(op.getParameters()).orElse(Collections.emptyList());
    if (!params.isEmpty()) {
      md.append("### Request Parameters\n");
      md.append(
          "| Name | In | Type | Required | Description |\n|------|----|------|-----------|-------------|\n");
      for (Parameter p : params) {
        Schema ps = getParameterSchema(openAPI, p);
        md.append("| `")
            .append(p.getName())
            .append("` | ")
            .append(p.getIn())
            .append(" | ")
            .append(getSchemaType(openAPI, ps))
            .append(" | ")
            .append(Boolean.TRUE.equals(p.getRequired()) ? "✅" : "❌")
            .append(" | ")
            .append(sanitizeMarkdown(p.getDescription()))
            .append(" |\n");
      }
      md.append("\n");
    }

    // Request Body (merged: single JSON5-style block in DSL)
    RequestBody rb = op.getRequestBody();
    if (rb != null) {
      Schema reqSchema = resolveRequestBodySchema(openAPI, rb);
      if (reqSchema != null) {
        md.append("### Request Body Schema\n");
        md.append("```json5\n");
        md.append(renderDSL(openAPI, reqSchema, 0, new HashSet<>(), true, null));
        md.append("\n```\n\n");
      }

      // Example request (simple convenience)
      md.append("### Example Request\n");
      md.append("```json\n").append(exampleRequestJson(openAPI, op)).append("\n```\n\n");
    }

    // Responses (each JSON body rendered in the same DSL)
    ApiResponses responses = op.getResponses();
    if (responses != null && !responses.isEmpty()) {
      responses.forEach(
          (status, response) -> {
            md.append("### Response `").append(status).append("`\n");
            md.append(sanitizeMarkdown(response.getDescription())).append("\n\n");
            if (response.getContent() != null
                && response.getContent().get("application/json") != null) {
              Schema resSchema =
                  resolveSchema(openAPI, response.getContent().get("application/json").getSchema());
              if (resSchema != null) {
                md.append("```json5\n");
                md.append(renderDSL(openAPI, resSchema, 0, new HashSet<>(), true, null));
                md.append("\n```\n\n");
              }
            }
          });
    }

    return Map.of(
        "operation",
        nz(op.getOperationId(), "unknownOperation"),
        "method",
        method,
        "path",
        path,
        "markdown",
        md.toString(),
        "summary",
        Objects.requireNonNullElse(op.getDescription(), op.getSummary()));
  }

  // ==============================================================
  // Nested DSL renderer (Object/Array/OneOf/AnyOf/AllOf/Ref/Enum/Format)
  // ==============================================================

  /**
   * Renders a schema in a compact, deeply nested DSL: Object({ "prop": <Type>, ... }) Array( <Type>
   * ) OneOf(A, B, ...) String(Enum("a","b")), String(date-time), Number, Integer, Boolean,
   * Ref(Name)
   *
   * @param inlineAsTop true: wrap top-level as Object({ .. })/Array(..)/Type; false: emit only the
   *     inner type
   * @param requiredContext if rendering children of an object, pass that object's 'required' set;
   *     otherwise null
   */
  private static String renderDSL(
      OpenAPI openAPI,
      Schema rawSchema,
      int indent,
      Set<String> seenRefs,
      boolean inlineAsTop,
      Set<String> requiredContext) {
    Schema schema = resolveSchema(openAPI, rawSchema);
    if (schema == null) return "Object({})";

    String ind = " ".repeat(indent);
    String t = schema.getType();

    // Cycle guard by ref key
    String rk = refKey(schema);
    if (rk != null) {
      if (seenRefs.contains(rk)) {
        return "Ref(" + rk + ")";
      }
      seenRefs.add(rk);
    }

    // $ref direct
    if (schema.get$ref() != null) {
      return "Ref(" + refName(schema.get$ref()) + ")";
    }

    // Composed types
    if (schema instanceof ComposedSchema) {
      ComposedSchema cs = (ComposedSchema) schema;
      String composed = composedLabel(openAPI, cs, indent, seenRefs);
      return composed;
    }

    // Arrays
    if (schema instanceof ArraySchema || "array".equals(t)) {
      Schema items = (schema instanceof ArraySchema) ? ((ArraySchema) schema).getItems() : null;
      if (items == null) items = new Schema<>(); // unknown items

      // Expand items inline (primitive/object/union)
      String itemRendered = renderDSL(openAPI, items, indent + 2, seenRefs, false, null);
      return "Array(\n" + ind + "  " + itemRendered + "\n" + ind + ")";
    }

    // Objects
    Map<String, Schema> props = schema.getProperties();
    if ("object".equals(t) || (props != null && !props.isEmpty())) {
      StringBuilder sb = new StringBuilder();
      if (inlineAsTop) sb.append("Object({\n");
      else sb.append("{\n");

      // Current level required fields
      Set<String> req = new LinkedHashSet<>();
      if (schema.getRequired() != null) req.addAll(schema.getRequired());

      if (props != null && !props.isEmpty()) {
        Iterator<Map.Entry<String, Schema>> it = props.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<String, Schema> e = it.next();
          String name = e.getKey();
          Schema child = resolveSchema(openAPI, e.getValue());

          // Comment line with (Required/Optional) + description
          String reqMark = req.contains(name) ? "(Required): " : "(Optional): ";
          String desc = oneLine(child.getDescription());
          if (desc.isEmpty()) desc = req.contains(name) ? "" : "";
          sb.append(ind).append("  // ").append(reqMark).append(desc).append("\n");

          // Field line
          sb.append(ind).append("  \"").append(name).append("\": ");

          // For child, decide rendering
          String renderedChild = renderChild(openAPI, child, indent + 2, seenRefs);
          sb.append(renderedChild);

          if (it.hasNext()) sb.append(",");
          sb.append("\n");
        }
      }

      if (inlineAsTop) sb.append(ind).append("})");
      else sb.append(ind).append("}");
      return sb.toString();
    }

    // Primitives
    return primitiveLabel(schema);
  }

  private static String renderChild(
      OpenAPI openAPI, Schema child, int indent, Set<String> seenRefs) {
    if (child == null) return "Object({})";
    String t = child.getType();
    if (child.get$ref() != null) return "Ref(" + refName(child.get$ref()) + ")";
    if (child instanceof ComposedSchema)
      return composedLabel(openAPI, (ComposedSchema) child, indent, seenRefs);

    // array (with description + flexible object handling)
    if (child instanceof ArraySchema || "array".equals(t)) {
      ArraySchema as = (ArraySchema) child;
      Schema items = resolveSchema(openAPI, as.getItems());
      if (items == null) items = new Schema<>();

      StringBuilder sb = new StringBuilder();
      sb.append("Array(\n");
      if (items.getDescription() != null && !items.getDescription().isEmpty()) {
        sb.append(" ".repeat(indent))
            .append("  // ")
            .append(oneLine(items.getDescription()))
            .append("\n");
      }

      if ("object".equals(items.getType())
          && (items.getProperties() == null || items.getProperties().isEmpty())
          && Boolean.TRUE.equals(items.getAdditionalProperties())) {
        sb.append(" ".repeat(indent)).append("  Object({additionalProperties})\n");
      } else if ("object".equals(items.getType())
          && (items.getProperties() == null || items.getProperties().isEmpty())) {
        sb.append(" ".repeat(indent)).append("  Object({})\n");
      } else {
        String itemRendered = renderDSL(openAPI, items, indent + 2, seenRefs, true, null);
        sb.append(" ".repeat(indent)).append("  ").append(itemRendered).append("\n");
      }

      sb.append(" ".repeat(indent)).append(")");
      return sb.toString();
    }

    // object
    Map<String, Schema> props = child.getProperties();
    if ("object".equals(t) || (props != null && !props.isEmpty())) {
      return renderDSL(
          openAPI,
          child,
          indent,
          seenRefs,
          true,
          child.getRequired() != null ? new LinkedHashSet<>(child.getRequired()) : null);
    }

    return primitiveLabel(child);
  }

  private static String composedLabel(
      OpenAPI openAPI, ComposedSchema cs, int indent, Set<String> seenRefs) {
    List<String> parts = new ArrayList<>();
    if (cs.getOneOf() != null && !cs.getOneOf().isEmpty()) {
      parts.add("OneOf(" + joinAlt(openAPI, cs.getOneOf(), indent, seenRefs) + ")");
    }
    if (cs.getAnyOf() != null && !cs.getAnyOf().isEmpty()) {
      parts.add("AnyOf(" + joinAlt(openAPI, cs.getAnyOf(), indent, seenRefs) + ")");
    }
    if (cs.getAllOf() != null && !cs.getAllOf().isEmpty()) {
      parts.add("AllOf(" + joinAlt(openAPI, cs.getAllOf(), indent, seenRefs) + ")");
    }
    return String.join(" & ", parts.isEmpty() ? Collections.singletonList("Object({})") : parts);
  }

  private static String joinAlt(
      OpenAPI openAPI, List<Schema> alts, int indent, Set<String> seenRefs) {
    List<String> labels = new ArrayList<>();
    for (Schema a : alts) {
      Schema s = resolveSchema(openAPI, a);
      if (s == null) {
        labels.add("Object({})");
        continue;
      }

      // Prefer compact for primitives, Ref(Name), or inline for objects/arrays
      if (s.get$ref() != null) {
        labels.add("Ref(" + refName(s.get$ref()) + ")");
        continue;
      }
      if (s instanceof ArraySchema || "array".equals(s.getType())) {
        Schema items = (s instanceof ArraySchema) ? ((ArraySchema) s).getItems() : null;
        String itemRendered = renderDSL(openAPI, items, indent + 2, seenRefs, false, null);
        labels.add("Array(" + itemRendered + ")");
        continue;
      }
      Map<String, Schema> props = s.getProperties();
      if ("object".equals(s.getType()) || (props != null && !props.isEmpty())) {
        labels.add(
            renderDSL(
                openAPI,
                s,
                indent,
                seenRefs,
                true,
                s.getRequired() != null ? new LinkedHashSet<>(s.getRequired()) : null));
        continue;
      }
      labels.add(primitiveLabel(s));
    }
    return String.join(", ", labels);
  }

  private static String primitiveLabel(Schema s) {
    if (s == null) return "String";
    if (s.get$ref() != null) return "Ref(" + refName(s.get$ref()) + ")";
    String t = nz(s.getType(), "string").toLowerCase(Locale.ROOT);
    String base =
        switch (t) {
          case "integer" -> "Integer";
          case "number" -> "Number";
          case "boolean" -> "Boolean";
          case "string" -> "String";
          case "object" -> "Object({})";
          case "array" -> "Array(Object({}))";
          default -> capitalize(t);
        };
    if (s.getEnum() != null && !s.getEnum().isEmpty()) {
      // String(Enum("a","b")) | Integer(Enum(1,2)) etc.
      String enumVals = joinEnumValues(s.getEnum());
      return base + "(Enum(" + enumVals + "))";
    }
    if (s.getFormat() != null && !s.getFormat().isEmpty() && "String".equals(base)) {
      return base + "(" + s.getFormat() + ")";
    }
    return base;
  }

  private static String joinEnumValues(List<?> enums) {
    List<String> out = new ArrayList<>();
    for (Object v : enums) {
      if (v == null) out.add("null");
      else if (v instanceof Number || v instanceof Boolean) out.add(String.valueOf(v));
      else out.add("\"" + String.valueOf(v) + "\"");
    }
    return String.join(", ", out);
  }

  // ==============================================================
  // Helpers (schema resolution, params, examples, utils)
  // ==============================================================

  private static Schema resolveRequestBodySchema(OpenAPI openAPI, RequestBody rb) {
    if (rb == null || rb.getContent() == null) return null;
    if (rb.getContent().containsKey("application/json")) {
      return resolveSchema(openAPI, rb.getContent().get("application/json").getSchema());
    }
    // fall back to vendor+json
    for (Map.Entry<String, MediaType> e : rb.getContent().entrySet()) {
      if (e.getKey() != null && e.getKey().toLowerCase(Locale.ROOT).contains("+json")) {
        return resolveSchema(openAPI, e.getValue().getSchema());
      }
    }
    return null;
  }

  private static Schema getParameterSchema(OpenAPI openAPI, Parameter p) {
    if (p == null) return null;
    if (p.getSchema() != null) return resolveSchema(openAPI, p.getSchema());
    if (p.getContent() != null && p.getContent().containsKey("application/json")) {
      return resolveSchema(openAPI, p.getContent().get("application/json").getSchema());
    }
    return null;
  }

  private static Schema resolveSchema(OpenAPI openAPI, Schema schema) {
    if (schema == null || schema.get$ref() == null) return schema;
    String name = refName(schema.get$ref());
    Components comps = openAPI.getComponents();
    if (name != null && comps != null && comps.getSchemas() != null) {
      Schema resolved = comps.getSchemas().get(name);
      if (resolved != null) return resolved;
    }
    return schema;
  }

  private static String getSchemaType(OpenAPI openAPI, Schema s) {
    if (s == null) return "object";
    s = resolveSchema(openAPI, s);
    if (s instanceof ArraySchema) {
      Schema items = resolveSchema(openAPI, ((ArraySchema) s).getItems());
      return "array[" + getSchemaType(openAPI, items) + "]";
    }
    if (s instanceof ComposedSchema) {
      List<String> parts = new ArrayList<>();
      ComposedSchema cs = (ComposedSchema) s;
      if (cs.getOneOf() != null && !cs.getOneOf().isEmpty()) parts.add("oneOf");
      if (cs.getAnyOf() != null && !cs.getAnyOf().isEmpty()) parts.add("anyOf");
      if (cs.getAllOf() != null && !cs.getAllOf().isEmpty()) parts.add("allOf");
      return parts.isEmpty() ? "composed" : String.join("|", parts);
    }
    String t = nz(s.getType(), "object");
    if (s.getFormat() != null) t += "(" + s.getFormat() + ")";
    if (s.getEnum() != null && !s.getEnum().isEmpty()) t += " enum";
    return t;
  }

  private static String exampleRequestJson(OpenAPI openAPI, Operation op) {
    StringBuilder sb = new StringBuilder("{\n");
    List<Parameter> params =
        Optional.ofNullable(op.getParameters()).orElse(Collections.emptyList());
    for (Parameter p : params) {
      Schema ps = getParameterSchema(openAPI, p);
      sb.append("  \"")
          .append(p.getName())
          .append("\": ")
          .append(primitiveExample(ps))
          .append(",\n");
    }
    RequestBody rb = op.getRequestBody();
    if (rb != null) {
      Schema bodySchema = resolveRequestBodySchema(openAPI, rb);
      if (bodySchema != null) sb.append("  \"body\": { ... }\n");
    }
    sb.append("}");
    return sb.toString();
  }

  private static String primitiveExample(Schema s) {
    if (s == null) return "\"string\"";
    if (s.getExample() != null) {
      Object ex = s.getExample();
      if (ex instanceof Number || ex instanceof Boolean) return String.valueOf(ex);
      return "\"" + ex + "\"";
    }
    String t = nz(s.getType(), "string");
    return switch (t) {
      case "integer" -> "0";
      case "number" -> "0.0";
      case "boolean" -> "true";
      default -> "\"string\"";
    };
  }

  private static String refKey(Schema s) {
    if (s == null) return null;
    if (s.getName() != null) return s.getName();
    if (s.get$ref() != null) return refName(s.get$ref());
    return null;
  }

  private static String refName(String ref) {
    if (ref == null) return null;
    int i = ref.lastIndexOf('/');
    return (i >= 0 && i + 1 < ref.length()) ? ref.substring(i + 1) : ref;
  }

  private static String oneLine(String s) {
    if (s == null) return "";
    return s.replace("\r", " ").replace("\n", " ").trim();
  }

  private static String capitalize(String s) {
    if (s == null || s.isEmpty()) return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  private static String sanitizeMarkdown(String text) {
    if (text == null || text.isEmpty()) return "";
    return text.replace("|", "\\|").replace("\r", "").replace("\n", "<br>").trim();
  }

  private static String nz(String v, String fallback) {
    return (v == null || v.isEmpty()) ? fallback : v;
  }
}
