package com.gentoro.onemcp.engine;

// A reasonably complete, safe-access JsonPath implementation in Java.
// NOTE: Full compliance with every edge case of the JsonPath spec is extensive.
// This implementation covers the core operators: $, ., [], wildcard '*', recursive descent '..',
// array slices, filters with comparison operators, existence checks, and safe null/type access.
// It uses Jackson for JSON parsing and node handling.

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonPath {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static JsonNode read(String json, String path) throws IOException {
    return read(MAPPER.readTree(json), path);
  }

  public static JsonNode read(JsonNode root, String path) throws IOException {
    // Support top-level boolean/relational expressions in addition to plain JsonPath
    if (looksLikeTopLevelExpression(path)) {
      try {
        return evalTopLevelExpression(root, path);
      } catch (Exception ignore) {
        // Fallback to standard path evaluation for backward compatibility
      }
    }

    List<JsonNode> result = evaluate(root, path);
    // For safety and test expectations we always return a non-null JsonNode.
    // When the path does not match anything, we return a JSON null node
    // instead of Java null so callers can safely use result.isNull().
    if (result.isEmpty()) return MAPPER.nullNode();

    // For filter expressions we always wrap in an array, even if a single
    // element matches. This matches the expectations of JsonPathTest,
    // which treats the result of filtered paths as an array.
    boolean isFilterPath = path != null && path.contains("[?(");
    if (isFilterPath) {
      return MAPPER.valueToTree(result);
    }

    if (result.size() == 1) return result.get(0);
    // If multiple nodes, return array
    return MAPPER.valueToTree(result);
  }

  // ---- Top-level boolean/relational expression support --------------------------------------

  /**
   * Fast heuristic to decide whether a string likely represents a top-level boolean expression
   * (e.g., "$.... == 10 && $.foo.size() > 0") rather than a plain JsonPath. We do not attempt to
   * prove it here; we try the expression parser and fall back to normal path on failure.
   */
  private static boolean looksLikeTopLevelExpression(String s) {
    if (s == null) return false;
    return hasTopLevelOperator(s);
  }

  private static boolean hasTopLevelOperator(String s) {
    int paren = 0, brack = 0;
    boolean inStr = false;
    char q = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (inStr) {
        if (c == '\\' && i + 1 < s.length()) {
          i++;
          continue;
        }
        if (c == q) {
          inStr = false;
          q = 0;
        }
        continue;
      }
      if (c == '\'' || c == '"') {
        inStr = true;
        q = c;
        continue;
      }
      if (c == '(') {
        paren++;
        continue;
      }
      if (c == ')') {
        if (paren > 0) paren--;
        continue;
      }
      if (c == '[') {
        brack++;
        continue;
      }
      if (c == ']') {
        if (brack > 0) brack--;
        continue;
      }
      if (paren == 0 && brack == 0) {
        // check two-char operators first
        if (i + 1 < s.length()) {
          String two = s.substring(i, i + 2);
          if (two.equals("&&")
              || two.equals("||")
              || two.equals("==")
              || two.equals("!=")
              || two.equals(">=")
              || two.equals("<=")) {
            return true;
          }
        }
        if (c == '<' || c == '>') return true;
        // Leading '!' as unary should also count as expression, but avoid paths like $.['not']
        if (c == '!') return true;
      }
    }
    return false;
  }

  private enum ExprTokType {
    LPAREN,
    RPAREN,
    AND,
    OR,
    NOT,
    EQ,
    NE,
    LT,
    GT,
    LE,
    GE,
    LITERAL,
    PATH,
    EOF
  }

  private static class ExprTok {
    ExprTokType type;
    String text; // for LITERAL (raw) or PATH
  }

  private static class ExprLexer {
    final String s;
    int i = 0;

    ExprLexer(String s) {
      this.s = s;
    }

    ExprTok next() {
      skipWs();
      if (i >= s.length()) return t(ExprTokType.EOF, null);
      char c = s.charAt(i);
      // Two-char operators
      if (match("&&")) return t(ExprTokType.AND, "&&");
      if (match("||")) return t(ExprTokType.OR, "||");
      if (match("==")) return t(ExprTokType.EQ, "==");
      if (match("!=")) return t(ExprTokType.NE, "!=");
      if (match(">=")) return t(ExprTokType.GE, ">=");
      if (match("<=")) return t(ExprTokType.LE, "<=");
      // Single-char
      if (c == '!') {
        i++;
        return t(ExprTokType.NOT, "!");
      }
      if (c == '<') {
        i++;
        return t(ExprTokType.LT, "<");
      }
      if (c == '>') {
        i++;
        return t(ExprTokType.GT, ">");
      }
      if (c == '(') {
        i++;
        return t(ExprTokType.LPAREN, "(");
      }
      if (c == ')') {
        i++;
        return t(ExprTokType.RPAREN, ")");
      }

      // String literal
      if (c == '"' || c == '\'') {
        char q = c;
        i++;
        StringBuilder sb = new StringBuilder();
        while (i < s.length()) {
          char ch = s.charAt(i++);
          if (ch == q) break;
          if (ch == '\\' && i < s.length()) {
            char esc = s.charAt(i++);
            sb.append(esc);
          } else {
            sb.append(ch);
          }
        }
        return t(ExprTokType.LITERAL, sb.toString());
      }

      // Path literal starting with $
      if (c == '$') {
        int start = i;
        // Read until a top-level operator boundary; we allow brackets/parentheses nesting
        int paren = 0;
        int brack = 0;
        while (i < s.length()) {
          char ch = s.charAt(i);
          if (ch == '(') paren++;
          else if (ch == ')') {
            if (paren == 0) break;
            paren--;
          } else if (ch == '[') brack++;
          else if (ch == ']') {
            if (brack > 0) brack--;
          }
          // If not inside brackets/parentheses, stop before operator tokens
          if (paren == 0 && brack == 0) {
            // stop before an operator or at end of a path token when encountering whitespace
            if (peekOpStart()) break;
          }
          i++;
        }
        // Trim any incidental trailing whitespace captured before an operator
        String pathText = s.substring(start, i).trim();
        return t(ExprTokType.PATH, pathText);
      }

      // Number or identifier literal (true/false/null)
      int start = i;
      while (i < s.length()) {
        char ch = s.charAt(i);
        if (Character.isLetterOrDigit(ch) || ch == '.' || ch == '_' || ch == '-') {
          i++;
        } else {
          break;
        }
      }
      if (i > start) {
        return t(ExprTokType.LITERAL, s.substring(start, i));
      }

      // Fallback: consume one char to avoid infinite loop
      i++;
      return t(ExprTokType.LITERAL, String.valueOf(c));
    }

    private boolean match(String op) {
      if (s.startsWith(op, i)) {
        i += op.length();
        return true;
      }
      return false;
    }

    private boolean peekOpStart() {
      // Detect start of a top-level operator; do NOT treat ')' as an operator boundary here,
      // because path lexing already tracks parentheses depth for functions like size().
      return s.startsWith("&&", i)
          || s.startsWith("||", i)
          || s.startsWith("==", i)
          || s.startsWith("!=", i)
          || s.startsWith(">=", i)
          || s.startsWith("<=", i)
          || s.charAt(i) == '<'
          || s.charAt(i) == '>';
    }

    private void skipWs() {
      while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }

    private static ExprTok t(ExprTokType type, String text) {
      ExprTok tok = new ExprTok();
      tok.type = type;
      tok.text = text;
      return tok;
    }
  }

  private static class ExprParser {
    final JsonNode root;
    final ExprLexer lx;
    ExprTok lookahead;

    ExprParser(JsonNode root, String s) {
      this.root = root;
      this.lx = new ExprLexer(s);
      this.lookahead = lx.next();
    }

    private void consume(ExprTokType t) {
      if (lookahead.type != t)
        throw new IllegalArgumentException("Expected " + t + " got " + lookahead.type);
      lookahead = lx.next();
    }

    JsonNode parse() throws IOException {
      return parseOr();
    }

    private JsonNode parseOr() throws IOException {
      JsonNode left = parseAnd();
      while (lookahead.type == ExprTokType.OR) {
        consume(ExprTokType.OR);
        JsonNode right = parseAnd();
        left = boolNode(truthy(left) || truthy(right));
      }
      return left;
    }

    private JsonNode parseAnd() throws IOException {
      JsonNode left = parseEquality();
      while (lookahead.type == ExprTokType.AND) {
        consume(ExprTokType.AND);
        // short-circuit
        if (!truthy(left)) {
          // still need to consume the rhs syntactically
          JsonNode skip = parseEquality();
          left = boolNode(false);
        } else {
          JsonNode right = parseEquality();
          left = boolNode(truthy(right));
        }
      }
      return left;
    }

    private JsonNode parseEquality() throws IOException {
      JsonNode left = parseRelational();
      while (lookahead.type == ExprTokType.EQ || lookahead.type == ExprTokType.NE) {
        ExprTokType op = lookahead.type;
        consume(op);
        JsonNode right = parseRelational();
        boolean eq = compareEq(left, right);
        left = boolNode(op == ExprTokType.EQ ? eq : !eq);
      }
      return left;
    }

    private JsonNode parseRelational() throws IOException {
      JsonNode left = parseUnary();
      while (lookahead.type == ExprTokType.LT
          || lookahead.type == ExprTokType.GT
          || lookahead.type == ExprTokType.LE
          || lookahead.type == ExprTokType.GE) {
        ExprTokType op = lookahead.type;
        consume(op);
        JsonNode right = parseUnary();
        int cmp = compareOrder(left, right);
        boolean r;
        switch (op) {
          case LT -> r = cmp < 0;
          case GT -> r = cmp > 0;
          case LE -> r = cmp <= 0;
          case GE -> r = cmp >= 0;
          default -> r = false;
        }
        left = boolNode(r);
      }
      return left;
    }

    private JsonNode parseUnary() throws IOException {
      if (lookahead.type == ExprTokType.NOT) {
        consume(ExprTokType.NOT);
        JsonNode v = parseUnary();
        return boolNode(!truthy(v));
      }
      return parsePrimary();
    }

    private JsonNode parsePrimary() throws IOException {
      switch (lookahead.type) {
        case LPAREN -> {
          consume(ExprTokType.LPAREN);
          JsonNode v = parseOr();
          consume(ExprTokType.RPAREN);
          return v;
        }
        case PATH -> {
          String p = lookahead.text;
          consume(ExprTokType.PATH);
          try {
            return JsonPath.read(root, p);
          } catch (Exception e) {
            return MAPPER.nullNode();
          }
        }
        case LITERAL -> {
          String lit = lookahead.text;
          consume(ExprTokType.LITERAL);
          return parseLiteral(lit);
        }
        default -> throw new IllegalArgumentException(
            "Unexpected token in expression: " + lookahead.type);
      }
    }

    private JsonNode parseLiteral(String lit) {
      if (lit == null) return MAPPER.nullNode();
      String t = lit.trim();
      if (t.equalsIgnoreCase("true")) return MAPPER.getNodeFactory().booleanNode(true);
      if (t.equalsIgnoreCase("false")) return MAPPER.getNodeFactory().booleanNode(false);
      if (t.equalsIgnoreCase("null")) return MAPPER.nullNode();
      // number
      try {
        if (t.contains(".")) return MAPPER.getNodeFactory().numberNode(Double.parseDouble(t));
        return MAPPER.getNodeFactory().numberNode(Long.parseLong(t));
      } catch (Exception ignored) {
      }
      // fallback string literal (already unquoted in lexer for quoted cases, but identifiers land
      // here)
      return TextNode.valueOf(t);
    }

    private boolean compareEq(JsonNode a, JsonNode b) {
      if (isNull(a) && isNull(b)) return true;
      if (isNull(a) || isNull(b)) return false;
      if (a.isNumber() && b.isNumber()) return Double.compare(a.asDouble(), b.asDouble()) == 0;
      if (a.isTextual() && b.isTextual()) return a.asText().equals(b.asText());
      if (a.isBoolean() && b.isBoolean()) return a.asBoolean() == b.asBoolean();
      return false;
    }

    private int compareOrder(JsonNode a, JsonNode b) {
      if (isNull(a) && isNull(b)) return 0;
      if (isNull(a)) return -1;
      if (isNull(b)) return 1;
      if (a.isNumber() && b.isNumber()) return Double.compare(a.asDouble(), b.asDouble());
      if (a.isTextual() && b.isTextual()) return a.asText().compareTo(b.asText());
      if (a.isBoolean() && b.isBoolean()) return Boolean.compare(a.asBoolean(), b.asBoolean());
      // Mixed or unsupported types: define a deterministic but conservative order via typeRank
      return Integer.compare(typeRank(a), typeRank(b));
    }
  }

  private static JsonNode evalTopLevelExpression(JsonNode root, String expr) throws IOException {
    ExprParser p = new ExprParser(root, expr);
    return p.parse();
  }

  private static boolean isNull(JsonNode n) {
    return n == null || n.isNull();
  }

  private static JsonNode boolNode(boolean b) {
    return MAPPER.getNodeFactory().booleanNode(b);
  }

  /** Truthiness consistent with ExecutionPlanEngine.asBoolean */
  private static boolean truthy(JsonNode v) {
    if (v == null || v.isNull()) return false;
    if (v.isBoolean()) return v.asBoolean();
    if (v.isNumber()) return v.asDouble() != 0.0;
    if (v.isTextual()) return !v.asText().isEmpty();
    if (v.isArray()) return v.size() > 0;
    if (v.isObject()) return v.size() > 0;
    return false;
  }

  public static List<JsonNode> evaluate(JsonNode root, String path) {
    if (path == null || path.isEmpty() || !path.startsWith("$")) {
      throw new IllegalArgumentException("JsonPath must start with '$'");
    }
    List<Token> tokens = tokenize(path);
    List<JsonNode> current = new ArrayList<>();
    current.add(root);

    for (Token token : tokens) {
      List<JsonNode> next = new ArrayList<>();
      for (JsonNode node : current) {
        next.addAll(applyToken(root, node, token));
      }
      current = next;
    }
    return current;
  }

  private static List<JsonNode> applyToken(JsonNode absoluteRoot, JsonNode node, Token token) {
    switch (token.type) {
      case FIELD:
        return selectField(node, token.value);
      case WILDCARD:
        return wildcard(node);
      case RECURSIVE:
        return recursive(node, token.value);
      case ARRAY_INDEX:
        return selectArrayIndex(node, token.indices);
      case ARRAY_SLICE:
        return selectArraySlice(node, token.sliceStart, token.sliceEnd, token.sliceStep);
      case FILTER:
        return filter(absoluteRoot, node, token.filterExpr);
      case FUNCTION:
        return applyFunction(node, token);
      default:
        return Collections.emptyList();
    }
  }

  private static List<JsonNode> applyFunction(JsonNode node, Token token) {
    String name = token.funcName == null ? "" : token.funcName.toLowerCase(Locale.ROOT);
    switch (name) {
      case "size":
        return List.of(functionSize(node));
      case "limit":
        int n = token.funcIntArg == null ? 0 : token.funcIntArg;
        return List.of(functionLimit(node, n));
      case "sort":
        return List.of(functionSort(node, token.funcArgRaw));
      default:
        // Unknown function: passthrough
        return List.of(node);
    }
  }

  private static JsonNode functionSize(JsonNode node) {
    if (node == null || node.isNull()) return MAPPER.getNodeFactory().numberNode(0);
    if (node.isArray()) return MAPPER.getNodeFactory().numberNode(node.size());
    if (node.isTextual()) return MAPPER.getNodeFactory().numberNode(node.asText().length());
    if (node.isObject()) return MAPPER.getNodeFactory().numberNode(node.size());
    // numbers/booleans/others → 0
    return MAPPER.getNodeFactory().numberNode(0);
  }

  private static JsonNode functionLimit(JsonNode node, int n) {
    if (n <= 0) {
      if (node != null && node.isTextual()) return TextNode.valueOf("");
      if (node != null && node.isArray()) return MAPPER.createArrayNode();
      return node; // passthrough for other types
    }
    if (node == null || node.isNull()) {
      // Return empty of the most common container types; otherwise null
      return MAPPER.createArrayNode();
    }
    if (node.isArray()) {
      ArrayNode out = MAPPER.createArrayNode();
      int limit = Math.min(n, node.size());
      for (int i = 0; i < limit; i++) out.add(node.get(i));
      return out;
    }
    if (node.isTextual()) {
      String s = node.asText();
      int limit = Math.min(n, s.length());
      return TextNode.valueOf(s.substring(0, limit));
    }
    // Other types: passthrough
    return node;
  }

  private static JsonNode functionSort(JsonNode node, String expr) {
    if (node == null || !node.isArray()) return node; // passthrough for non-arrays
    // Build list with original indices for stable sort
    List<Map.Entry<Integer, JsonNode>> items = new ArrayList<>();
    for (int i = 0; i < node.size(); i++) {
      items.add(new AbstractMap.SimpleEntry<>(i, node.get(i)));
    }
    final String keyExpr = expr == null ? "" : expr.trim();
    items.sort(
        (a, b) -> {
          JsonNode ka = extractSortKey(a.getValue(), keyExpr);
          JsonNode kb = extractSortKey(b.getValue(), keyExpr);
          int cmp = compareSortKeys(ka, kb);
          if (cmp != 0) return cmp;
          // stable fallback to original index
          return Integer.compare(a.getKey(), b.getKey());
        });
    ArrayNode out = MAPPER.createArrayNode();
    for (Map.Entry<Integer, JsonNode> e : items) out.add(e.getValue());
    return out;
  }

  private static JsonNode extractSortKey(JsonNode element, String expr) {
    if (expr == null || expr.isEmpty()) return element; // sort by element as-is
    try {
      // Evaluate with the element as the root
      return JsonPath.read(element, expr);
    } catch (Exception e) {
      return MAPPER.nullNode();
    }
  }

  private static int typeRank(JsonNode n) {
    if (n == null || n.isNull()) return 5;
    if (n.isNumber()) return 0;
    if (n.isTextual()) return 1;
    if (n.isBoolean()) return 2;
    if (n.isArray() || n.isObject()) return 4;
    return 3; // other primitives
  }

  private static int compareSortKeys(JsonNode a, JsonNode b) {
    int ra = typeRank(a);
    int rb = typeRank(b);
    if (ra != rb) return Integer.compare(ra, rb);
    if (a == null || a.isNull()) return 0; // both null
    if (a.isNumber()) return Double.compare(a.asDouble(), b.asDouble());
    if (a.isTextual()) return a.asText().compareTo(b.asText());
    if (a.isBoolean()) return Boolean.compare(a.asBoolean(), b.asBoolean());
    // For arrays/objects/others: use size or toString as a fallback
    if (a.isArray() || a.isObject()) return Integer.compare(a.size(), b.size());
    return a.toString().compareTo(b.toString());
  }

  private static List<JsonNode> selectField(JsonNode node, String field) {
    if (!node.isObject()) return Collections.emptyList();
    JsonNode result = node.get(field);
    return result == null ? Collections.emptyList() : List.of(result);
  }

  private static List<JsonNode> wildcard(JsonNode node) {
    if (node.isObject()) {
      List<JsonNode> result = new ArrayList<>();
      node.fields().forEachRemaining(e -> result.add(e.getValue()));
      return result;
    }
    if (node.isArray()) {
      List<JsonNode> result = new ArrayList<>();
      node.forEach(result::add);
      return result;
    }
    return Collections.emptyList();
  }

  private static List<JsonNode> recursive(JsonNode node, String field) {
    List<JsonNode> result = new ArrayList<>();
    if ("*".equals(field)) {
      // Deep wildcard: collect all descendant nodes
      traverseAll(node, result);
    } else {
      traverse(node, field, result);
    }
    return result;
  }

  private static void traverse(JsonNode node, String field, List<JsonNode> out) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              e -> {
                if (e.getKey().equals(field)) out.add(e.getValue());
                traverse(e.getValue(), field, out);
              });
    } else if (node.isArray()) {
      node.forEach(n -> traverse(n, field, out));
    }
  }

  // Helper for deep wildcard `$..*`: add every descendant node
  private static void traverseAll(JsonNode node, List<JsonNode> out) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              e -> {
                JsonNode child = e.getValue();
                out.add(child);
                traverseAll(child, out);
              });
    } else if (node.isArray()) {
      node.forEach(
          child -> {
            out.add(child);
            traverseAll(child, out);
          });
    }
  }

  private static List<JsonNode> selectArrayIndex(JsonNode node, List<Integer> indices) {
    if (!node.isArray()) return Collections.emptyList();
    List<JsonNode> result = new ArrayList<>();
    for (Integer idx : indices) {
      if (idx >= 0 && idx < node.size()) {
        result.add(node.get(idx));
      }
    }
    return result;
  }

  private static List<JsonNode> selectArraySlice(JsonNode node, int start, int end, int step) {
    if (!node.isArray()) return Collections.emptyList();
    List<JsonNode> result = new ArrayList<>();
    int size = node.size();
    int s = start < 0 ? 0 : start;
    int e = end < 0 || end > size ? size : end;
    for (int i = s; i < e; i += step) {
      result.add(node.get(i));
    }
    return result;
  }

  // Enhanced filter: first try the advanced predicate evaluator supporting &&, ||, nested paths and
  // $ vs @.
  // Falls back to legacy simple forms for backward compatibility if needed.
  private static List<JsonNode> filter(JsonNode absoluteRoot, JsonNode node, String expr) {
    if (!node.isArray()) return Collections.emptyList();
    List<JsonNode> result = new ArrayList<>();
    String trimmed = expr.trim();

    // Try advanced evaluator first
    try {
      JsonPathResolver resolver = new JsonPathResolver();
      for (JsonNode item : node) {
        boolean ok = FilterPredicateEvaluator.evaluate(trimmed, item, absoluteRoot, resolver);
        if (ok) result.add(item);
      }
      return result;
    } catch (Throwable ignored) {
      // Fallback to legacy simple regex-based filter handling below
      result.clear();
    }

    // First handle existence predicate: ?(@.field)
    Pattern existsPattern = Pattern.compile("@\\.(\\w+)");
    Matcher existsMatcher = existsPattern.matcher(trimmed);
    if (existsMatcher.matches()) {
      String field = existsMatcher.group(1);
      for (JsonNode item : node) {
        JsonNode f = item.get(field);
        if (f != null && !f.isNull()) {
          result.add(item);
        }
      }
      return result;
    }

    // Comparison predicates: ?(@.field <op> literal)
    // Note: multi-character operators MUST be listed before single-character
    // ones, otherwise expressions like ">=" would be tokenized as ">" and
    // leave "= 200" as part of the value, breaking numeric parsing.
    Pattern p = Pattern.compile("@\\.(\\w+)\\s*(==|!=|<=|>=|<|>)\\s*(.*)");
    Matcher m = p.matcher(trimmed);
    if (!m.matches()) return Collections.emptyList();

    String field = m.group(1);
    String op = m.group(2);
    String value = m.group(3);

    for (JsonNode item : node) {
      JsonNode f = item.get(field);
      if (f == null || f.isNull()) continue;
      if (compare(f, op, value)) result.add(item);
    }
    return result;
  }

  private static boolean compare(JsonNode f, String op, String value) {
    String lit = stripQuotes(value);
    if (f.isNumber()) {
      double left = f.asDouble();
      double right;
      try {
        right = Double.parseDouble(lit);
      } catch (Exception e) {
        return false;
      }
      return switch (op) {
        case "==" -> left == right;
        case "!=" -> left != right;
        case "<" -> left < right;
        case ">" -> left > right;
        case "<=" -> left <= right;
        case ">=" -> left >= right;
        default -> false;
      };
    } else if (f.isTextual()) {
      int cmp = f.asText().compareTo(lit);
      return switch (op) {
        case "==" -> f.asText().equals(lit);
        case "!=" -> !f.asText().equals(lit);
        case "<" -> cmp < 0;
        case ">" -> cmp > 0;
        case "<=" -> cmp <= 0;
        case ">=" -> cmp >= 0;
        default -> false;
      };
    }
    return false;
  }

  private static String stripQuotes(String s) {
    s = s.trim();
    if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  // Tokenization logic
  private enum Type {
    FIELD,
    WILDCARD,
    RECURSIVE,
    ARRAY_INDEX,
    ARRAY_SLICE,
    FILTER,
    FUNCTION
  }

  private static class Token {
    Type type;
    String value;
    List<Integer> indices;
    int sliceStart;
    int sliceEnd;
    int sliceStep;
    String filterExpr;
    // Function support
    String funcName;
    String funcArgRaw; // raw inside parentheses, e.g. for sort(expr)
    Integer funcIntArg; // for limit(n)
  }

  private static List<Token> tokenize(String path) {
    // Very simplified tokenizer for JsonPath. A production implementation would be more complex.
    List<Token> tokens = new ArrayList<>();
    String p = path.substring(1); // remove $
    int i = 0;
    while (i < p.length()) {
      char c = p.charAt(i);
      if (c == '.') {
        // If a dot is immediately followed by a bracket, e.g. ".[0]" or
        // "['field']", we treat the dot as a no-op separator and let the
        // bracket logic handle the next token. This allows both
        //   $.items[0].name
        // and
        //   $.items.[0].name
        // to work the same way.
        if (i + 1 < p.length() && p.charAt(i + 1) == '[') {
          i++; // skip the '.' and re-process the '[' on next iteration
          continue;
        }

        if (i + 1 < p.length() && p.charAt(i + 1) == '.') {
          int start = i + 2;
          int end = readFieldName(p, start);
          String field = p.substring(start, end);
          Token t = new Token();
          t.type = Type.RECURSIVE;
          t.value = field;
          tokens.add(t);
          i = end;
        } else {
          int start = i + 1;
          int end;
          // Attempt to parse a function segment: <name>(...)
          int j = start;
          while (j < p.length()) {
            char ch = p.charAt(j);
            if (Character.isLetterOrDigit(ch) || ch == '_') {
              j++;
              continue;
            }
            break;
          }
          if (j < p.length() && p.charAt(j) == '(') {
            // parse until matching ')'
            int k = j;
            int depth = 0;
            boolean closed = false;
            while (k < p.length()) {
              char ch2 = p.charAt(k);
              if (ch2 == '(') depth++;
              else if (ch2 == ')') {
                depth--;
                if (depth == 0) {
                  closed = true;
                  break;
                }
              }
              k++;
            }
            if (closed) {
              String fname = p.substring(start, j);
              String fargs = p.substring(j + 1, k);
              Token t = new Token();
              t.type = Type.FUNCTION;
              t.funcName = fname;
              t.funcArgRaw = fargs == null ? "" : fargs.trim();
              if ("limit".equals(fname)) {
                try {
                  t.funcIntArg = Integer.parseInt(t.funcArgRaw.replaceAll("\\s+", ""));
                } catch (Exception ignored) {
                  t.funcIntArg = 0;
                }
              }
              tokens.add(t);
              end = k + 1;
              i = end;
              continue;
            }
          }
          // Not a function: fall back to normal field/number token
          end = readFieldName(p, start);
          String field = p.substring(start, end);
          Token t = new Token();
          if (field.equals("*")) {
            t.type = Type.WILDCARD;
          } else if (isAllDigits(field)) {
            t.type = Type.ARRAY_INDEX;
            t.indices = new ArrayList<>();
            try {
              t.indices.add(Integer.parseInt(field));
            } catch (NumberFormatException ignored) {
              t.type = Type.FIELD;
              t.value = field;
            }
          } else {
            t.type = Type.FIELD;
            t.value = field;
          }
          tokens.add(t);
          i = end;
        }
      } else if (c == '[') {
        int end = p.indexOf(']', i);
        if (end < 0) throw new IllegalArgumentException("Unclosed '['");
        String inside = p.substring(i + 1, end);
        Token t = parseBracket(inside);
        tokens.add(t);
        i = end + 1;
      } else {
        i++;
      }
    }
    return tokens;
  }

  /** Returns true if the given segment consists only of ASCII digits. */
  private static boolean isAllDigits(String s) {
    if (s == null || s.isEmpty()) return false;
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch < '0' || ch > '9') return false;
    }
    return true;
  }

  private static int readFieldName(String p, int start) {
    int i = start;
    while (i < p.length()) {
      char c = p.charAt(i);
      if (c == '.' || c == '[') break;
      i++;
    }
    return i;
  }

  private static Token parseBracket(String inside) {
    Token t = new Token();
    inside = inside.trim();

    // Support quoted field names inside brackets, e.g. ['product.category'] or ["foo.bar"].
    // In this case we treat the content as a literal field name rather than an array index,
    // which allows accessing flat keys that contain dots without interpreting them as
    // nested paths.
    if ((inside.startsWith("'") && inside.endsWith("'"))
        || (inside.startsWith("\"") && inside.endsWith("\""))) {
      t.type = Type.FIELD;
      t.value = stripQuotes(inside);
      return t;
    }

    if (inside.equals("*")) {
      t.type = Type.WILDCARD;
      return t;
    }

    if (inside.startsWith("?(") && inside.endsWith(")")) {
      t.type = Type.FILTER;
      t.filterExpr = inside.substring(2, inside.length() - 1);
      return t;
    }

    if (inside.contains(":")) {
      t.type = Type.ARRAY_SLICE;
      String[] parts = inside.split(":");
      t.sliceStart = parts[0].isEmpty() ? 0 : Integer.parseInt(parts[0]);
      t.sliceEnd = parts.length > 1 && !parts[1].isEmpty() ? Integer.parseInt(parts[1]) : -1;
      t.sliceStep = parts.length > 2 && !parts[2].isEmpty() ? Integer.parseInt(parts[2]) : 1;
      return t;
    }

    t.type = Type.ARRAY_INDEX;
    String[] idx = inside.split(",");
    t.indices = new ArrayList<>();
    for (String s : idx) {
      try {
        t.indices.add(Integer.parseInt(s.trim()));
      } catch (Exception ignored) {
      }
    }
    return t;
  }
}
