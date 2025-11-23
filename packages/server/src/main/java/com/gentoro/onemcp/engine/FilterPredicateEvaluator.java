package com.gentoro.onemcp.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Minimal boolean predicate parser/evaluator for JSONPath-style filter expressions.
 *
 * <p>This class implements a tiny, dependency-free parser to evaluate boolean predicates used by
 * {@code filter} nodes inside the {@link ExecutionPlanEngine}. Its goal is to cover the most common
 * cases of JSONPath filter semantics without pulling a full JSONPath library capable of evaluating
 * arbitrary filter expressions.
 *
 * <p>Supported features:
 *
 * <ul>
 *   <li>Logical operators: {@code &&}, {@code ||} with standard precedence (AND binds tighter).
 *   <li>Parentheses for grouping.
 *   <li>Comparisons: {@code ==}, {@code !=}, {@code <}, {@code <=}, {@code >}, {@code >=}.
 *   <li>Literals: numbers, strings (double quotes), booleans ({@code true}/{@code false}), {@code
 *       null}.
 *   <li>Paths: starting with {@code @} (item-root) or {@code $} (absolute/global). Paths are
 *       delegated to {@link JsonPathResolver} for resolution.
 *   <li>Truthiness for non-comparison values when used as boolean (empty string/zero/null → false).
 * </ul>
 *
 * <p>Examples:
 *
 * <pre>{@code
 * @.age >= 21 && @.active
 * $[?(@.status == "PAID" || @.total > 100)]
 * @.name == "Alice"
 * }</pre>
 *
 * <p>Non-goals and limitations:
 *
 * <ul>
 *   <li>No functions (e.g., {@code length()}, {@code contains()}).
 *   <li>No array slicing; path parsing here is lenient and delegated to the resolver.
 *   <li>No implicit type coercion beyond what is described in {@link Value} and {@link #compare}.
 * </ul>
 */
final class FilterPredicateEvaluator {

  private final String s;
  private final int n;
  private int i;
  private final JsonNode itemRoot;
  private final JsonNode absoluteRoot;
  private final JsonPathResolver resolver;

  private FilterPredicateEvaluator(
      String expr, JsonNode itemRoot, JsonNode absoluteRoot, JsonPathResolver resolver) {
    this.s = expr;
    this.n = expr.length();
    this.i = 0;
    this.itemRoot = itemRoot == null ? JsonNodeFactory.instance.nullNode() : itemRoot;
    this.absoluteRoot = absoluteRoot == null ? this.itemRoot : absoluteRoot;
    this.resolver = resolver;
  }

  /**
   * Entry point used by the engine.
   *
   * @param expr predicate string (plain, or wrapped as {@code $[?( ... )]}).
   * @param itemRoot the current item being tested (root for {@code @} paths).
   * @param resolver JSONPath resolver used to fetch values for paths.
   * @return true if the predicate evaluates to truthy; false otherwise.
   */
  static boolean evaluate(String expr, JsonNode itemRoot, JsonPathResolver resolver) {
    // Backward-compatible overload: when absoluteRoot is not provided, use itemRoot
    FilterPredicateEvaluator p =
        new FilterPredicateEvaluator(normalize(expr), itemRoot, itemRoot, resolver);
    boolean val = p.parseOr();
    p.skipWs();
    return val;
  }

  /**
   * Enhanced evaluation entry point that distinguishes between item-root (@) and absolute root ($).
   *
   * <p>Use this when the predicate may reference global paths via '$' while iterating array items
   * as '@'.
   */
  static boolean evaluate(
      String expr, JsonNode itemRoot, JsonNode absoluteRoot, JsonPathResolver resolver) {
    FilterPredicateEvaluator p =
        new FilterPredicateEvaluator(normalize(expr), itemRoot, absoluteRoot, resolver);
    boolean val = p.parseOr();
    p.skipWs();
    return val;
  }

  /**
   * Accept both compact expressions and full JSONPath filter wrappers like {@code $[?( ... )]}.
   * Returns the inner predicate content for the parser.
   */
  private static String normalize(String expr) {
    String e = expr.trim();
    // Accept full JSONPath filter syntax: $[?( ... )]
    if (e.startsWith("$[?(") && e.endsWith(")]")) {
      // extract inside of ?( ... ) with balanced parentheses
      int open = e.indexOf("?(") + 2; // position after '?('
      int depth = 0;
      for (int j = open; j < e.length(); j++) {
        char c = e.charAt(j);
        if (c == '(') {
          depth++;
        } else if (c == ')') {
          if (depth == 0) {
            return e.substring(open, j);
          }
          depth--;
        }
      }
      // fallback: strip outer '$[?(' prefix and ')]' suffix
      return e.substring(open, e.length() - 2);
    }
    // If it starts with @ without leading parentheses, allow directly
    return e;
  }

  // Grammar: or := and ('||' and)*
  private boolean parseOr() {
    boolean left = parseAnd();
    skipWs();
    while (match("||")) {
      boolean right = parseAnd();
      left = left || right;
      skipWs();
    }
    return left;
  }

  // Grammar: and := cmp ('&&' cmp)*
  private boolean parseAnd() {
    boolean left = parseComparisonOrPrimary();
    skipWs();
    while (match("&&")) {
      boolean right = parseComparisonOrPrimary();
      left = left && right;
      skipWs();
    }
    return left;
  }

  // Grammar: cmp := value (op value)? | '(' or ')'
  private boolean parseComparisonOrPrimary() {
    // Lookahead: expr [op expr]
    Value a = parseValueOrGroup();
    skipWs();
    String op = readAny("==", "!=", "<=", ">=", "<", ">");
    if (op != null) {
      Value b = parseValueOrGroup();
      return compare(op, a, b);
    }
    // No comparison: use truthiness of 'a'
    return truthy(a);
  }

  /** Parse a value: group, string, number, keyword, or path. */
  private Value parseValueOrGroup() {
    skipWs();
    if (match("(")) {
      boolean inner = parseOr();
      expect(")");
      return Value.ofBoolean(inner);
    }
    // string literal
    if (peek() == '"') {
      return Value.ofString(readString());
    }
    // number literal
    if (isNumStart(peek())) {
      String num = readNumber();
      try {
        if (num.contains(".") || num.contains("e") || num.contains("E")) {
          return Value.ofNumber(Double.parseDouble(num));
        }
        return Value.ofNumber(Long.parseLong(num));
      } catch (NumberFormatException ex) {
        return Value.nullValue();
      }
    }
    // keywords
    String kw = readKeyword();
    if (kw != null) {
      return switch (kw) {
        case "true" -> Value.ofBoolean(true);
        case "false" -> Value.ofBoolean(false);
        case "null" -> Value.nullValue();
        default -> Value.nullValue();
      };
    }
    // path @... or $...
    if (peek() == '@' || peek() == '$') {
      String path = readPath();
      if (path.startsWith("@")) {
        // Convert @. to $. for our resolver which expects $ root, using the item as root
        String pth = "$" + path.substring(1);
        JsonNode v = resolver.read(itemRoot, pth);
        if (v == null || v.isNull()) return Value.nullValue();
        if (v.isNumber()) return Value.ofNumber(v.numberValue());
        if (v.isBoolean()) return Value.ofBoolean(v.asBoolean());
        if (v.isTextual()) return Value.ofString(v.asText());
        return Value.ofJson(v);
      }
      // '$' path: resolve against the absolute root
      JsonNode v = resolver.read(absoluteRoot, path);
      if (v == null || v.isNull()) return Value.nullValue();
      if (v.isNumber()) return Value.ofNumber(v.numberValue());
      if (v.isBoolean()) return Value.ofBoolean(v.asBoolean());
      if (v.isTextual()) return Value.ofString(v.asText());
      return Value.ofJson(v);
    }
    // Fallback: null
    return Value.nullValue();
  }

  /**
   * Compare two values with JSONPath-like semantics:
   *
   * <ul>
   *   <li>Numeric comparisons when both sides are numbers
   *   <li>Equality for other types via string/boolean/null comparison
   *   <li>Ordering for non-numeric types via string comparison
   * </ul>
   */
  private boolean compare(String op, Value a, Value b) {
    // If both numbers, numeric compare; else string/boolean equality/ordering (ordering only
    // numeric)
    if (a.isNumber() && b.isNumber()) {
      double da = a.asDouble();
      double db = b.asDouble();
      return switch (op) {
        case "==" -> Double.compare(da, db) == 0;
        case "!=" -> Double.compare(da, db) != 0;
        case "<" -> da < db;
        case "<=" -> da <= db;
        case ">" -> da > db;
        case ">=" -> da >= db;
        default -> false;
      };
    }
    // Equality for other types via string/boolean/null/stringified
    if (op.equals("==") || op.equals("!=")) {
      boolean eq = a.equalsTo(b);
      return op.equals("==") ? eq : !eq;
    }
    // Non-numeric ordering: compare string representations
    int cmp = a.asString().compareTo(b.asString());
    return switch (op) {
      case "<" -> cmp < 0;
      case "<=" -> cmp <= 0;
      case ">" -> cmp > 0;
      case ">=" -> cmp >= 0;
      default -> false;
    };
  }

  /** Determine truthiness similar to JavaScript-ish semantics over {@link Value}. */
  private boolean truthy(Value v) {
    if (v.isBoolean()) return v.asBoolean();
    if (v.isNull()) return false;
    if (v.isNumber()) return v.asDouble() != 0.0;
    if (v.isString()) return !v.asString().isEmpty();
    // JSON value: arrays/objects: true if non-empty
    if (v.isJson()) {
      JsonNode j = v.asJson();
      if (j.isArray() || j.isObject()) return j.size() > 0;
      if (j.isBoolean()) return j.asBoolean();
      if (j.isNumber()) return j.asDouble() != 0.0;
      if (j.isTextual()) return !j.asText().isEmpty();
      return !j.isNull();
    }
    return false;
  }

  // Lexer helpers
  private void skipWs() {
    while (i < n && Character.isWhitespace(s.charAt(i))) i++;
  }

  private boolean match(String token) {
    skipWs();
    if (s.startsWith(token, i)) {
      i += token.length();
      return true;
    }
    return false;
  }

  private void expect(String token) {
    if (!match(token))
      throw new ExecutionPlanException("Expected '" + token + "' in filter predicate");
  }

  private char peek() {
    skipWs();
    return i < n ? s.charAt(i) : '\0';
  }

  private String readAny(String... ops) {
    skipWs();
    for (String op : ops) {
      if (s.startsWith(op, i)) {
        i += op.length();
        return op;
      }
    }
    return null;
  }

  private boolean isNumStart(char c) {
    return (c == '-' || Character.isDigit(c));
  }

  private String readNumber() {
    skipWs();
    int start = i;
    if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
    while (i < n && Character.isDigit(s.charAt(i))) i++;
    if (i < n && s.charAt(i) == '.') {
      i++;
      while (i < n && Character.isDigit(s.charAt(i))) i++;
    }
    if (i < n && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
      i++;
      if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
      while (i < n && Character.isDigit(s.charAt(i))) i++;
    }
    return s.substring(start, i);
  }

  private String readString() {
    skipWs();
    if (s.charAt(i) != '"') throw new ExecutionPlanException("Expected string literal");
    i++; // skip opening quote
    StringBuilder sb = new StringBuilder();
    while (i < n) {
      char c = s.charAt(i++);
      if (c == '"') break;
      if (c == '\\' && i < n) {
        char e = s.charAt(i++);
        switch (e) {
          case '"' -> sb.append('"');
          case '\\' -> sb.append('\\');
          case '/' -> sb.append('/');
          case 'b' -> sb.append('\b');
          case 'f' -> sb.append('\f');
          case 'n' -> sb.append('\n');
          case 'r' -> sb.append('\r');
          case 't' -> sb.append('\t');
          case 'u' -> {
            if (i + 3 < n) {
              String hex = s.substring(i, i + 4);
              sb.append((char) Integer.parseInt(hex, 16));
              i += 4;
            }
          }
          default -> sb.append(e);
        }
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private String readKeyword() {
    skipWs();
    int start = i;
    while (i < n && Character.isLetter(s.charAt(i))) i++;
    if (start == i) return null;
    return s.substring(start, i);
  }

  /**
   * Read a permissive path token beginning with {@code @} or {@code $}. The actual resolution rules
   * are delegated to {@link JsonPathResolver}.
   */
  private String readPath() {
    skipWs();
    int start = i;
    if (i < n && (s.charAt(i) == '@' || s.charAt(i) == '$')) i++;
    while (i < n) {
      char c = s.charAt(i);
      if (Character.isLetterOrDigit(c)
          || c == '_'
          || c == '.'
          || c == '['
          || c == ']'
          || c == '"'
          || c == '\''
          || c == '-') {
        i++;
      } else {
        break;
      }
    }
    return s.substring(start, i);
  }

  // Small value wrapper to simplify type ops
  private static final class Value {
    private final Object v;
    private final Type t;

    enum Type {
      BOOLEAN,
      NUMBER,
      STRING,
      NULL,
      JSON
    }

    private Value(Object v, Type t) {
      this.v = v;
      this.t = t;
    }

    static Value ofBoolean(boolean b) {
      return new Value(b, Type.BOOLEAN);
    }

    static Value ofNumber(Number n) {
      return new Value(n.doubleValue(), Type.NUMBER);
    }

    static Value ofString(String s) {
      return new Value(s, Type.STRING);
    }

    static Value nullValue() {
      return new Value(null, Type.NULL);
    }

    static Value ofJson(JsonNode n) {
      return new Value(n, Type.JSON);
    }

    boolean isBoolean() {
      return t == Type.BOOLEAN;
    }

    boolean isNumber() {
      return t == Type.NUMBER;
    }

    boolean isString() {
      return t == Type.STRING;
    }

    boolean isNull() {
      return t == Type.NULL;
    }

    boolean isJson() {
      return t == Type.JSON;
    }

    boolean asBoolean() {
      return Boolean.TRUE.equals(v);
    }

    double asDouble() {
      return ((Number) v).doubleValue();
    }

    String asString() {
      return v == null ? "" : v.toString();
    }

    JsonNode asJson() {
      return (JsonNode) v;
    }

    /**
     * Equality with lenient cross-type support:
     *
     * <ul>
     *   <li>JSON nodes are compared by their string representation (simple and predictable).
     *   <li>Numbers by numeric equality.
     *   <li>Other primitives by string equality.
     * </ul>
     */
    boolean equalsTo(Value other) {
      if (this.t == Type.JSON || other.t == Type.JSON) {
        // Compare via string for simplicity
        return this.asString().equals(other.asString());
      }
      if (this.t == Type.NUMBER && other.t == Type.NUMBER) {
        return Double.compare(this.asDouble(), other.asDouble()) == 0;
      }
      return this.asString().equals(other.asString());
    }
  }
}
