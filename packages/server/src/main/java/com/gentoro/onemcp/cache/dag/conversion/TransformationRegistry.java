package com.gentoro.onemcp.cache.dag.conversion;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Registry for value transformation functions.
 * 
 * <p>Supports various transformation types:
 * - Type conversions: str->int, int->str, str->bool, etc.
 * - Regex transformations: replace, extract, match
 * - String operations: uppercase, lowercase, trim, substring
 * - Number formatting: decimal places, padding, etc.
 * - Date/time formatting (delegated to ValueConverter)
 * 
 * <p>Transformations are registered by name and can be chained.
 */
public class TransformationRegistry {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(TransformationRegistry.class);

  private final Map<String, Function<Object, Object>> transformations = new HashMap<>();

  public TransformationRegistry() {
    registerBuiltInTransformations();
  }

  /**
   * Register a transformation function.
   * 
   * @param name transformation name (e.g., "str_to_int", "regex_replace")
   * @param transformation function that takes input value and returns transformed value
   */
  public void register(String name, Function<Object, Object> transformation) {
    transformations.put(name, transformation);
  }

  /**
   * Apply a transformation by name.
   * 
   * @param name transformation name
   * @param value input value
   * @return transformed value, or original value if transformation not found
   */
  public Object apply(String name, Object value) {
    Function<Object, Object> transformation = transformations.get(name);
    if (transformation == null) {
      log.warn("Transformation '{}' not found, returning original value", name);
      return value;
    }
    
    try {
      return transformation.apply(value);
    } catch (Exception e) {
      log.warn("Transformation '{}' failed for value '{}': {}", name, value, e.getMessage());
      return value; // Fallback to original value
    }
  }

  /**
   * Check if a transformation is registered.
   */
  public boolean hasTransformation(String name) {
    return transformations.containsKey(name);
  }

  /**
   * Register built-in transformations.
   */
  private void registerBuiltInTransformations() {
    // Type conversions: String <-> Number (using Apache Commons Lang)
    register("str_to_int", value -> {
      if (value == null) return null;
      String str = StringUtils.trimToEmpty(value.toString());
      if (str.isEmpty()) return 0;
      if (NumberUtils.isCreatable(str)) {
        return NumberUtils.toInt(str, 0);
      }
      throw new IllegalArgumentException("Cannot convert '" + str + "' to integer");
    });

    register("str_to_long", value -> {
      if (value == null) return null;
      String str = StringUtils.trimToEmpty(value.toString());
      if (str.isEmpty()) return 0L;
      if (NumberUtils.isCreatable(str)) {
        return NumberUtils.toLong(str, 0L);
      }
      throw new IllegalArgumentException("Cannot convert '" + str + "' to long");
    });

    register("str_to_double", value -> {
      if (value == null) return null;
      String str = StringUtils.trimToEmpty(value.toString());
      if (str.isEmpty()) return 0.0;
      if (NumberUtils.isCreatable(str)) {
        return NumberUtils.toDouble(str, 0.0);
      }
      throw new IllegalArgumentException("Cannot convert '" + str + "' to double");
    });

    register("str_to_float", value -> {
      if (value == null) return null;
      String str = StringUtils.trimToEmpty(value.toString());
      if (str.isEmpty()) return 0.0f;
      if (NumberUtils.isCreatable(str)) {
        return NumberUtils.toFloat(str, 0.0f);
      }
      throw new IllegalArgumentException("Cannot convert '" + str + "' to float");
    });

    register("int_to_str", value -> {
      if (value == null) return null;
      if (value instanceof Number) {
        return String.valueOf(((Number) value).longValue());
      }
      return value.toString();
    });

    register("double_to_str", value -> {
      if (value == null) return null;
      if (value instanceof Number) {
        return String.valueOf(((Number) value).doubleValue());
      }
      return value.toString();
    });

    // Boolean conversions
    register("str_to_bool", value -> {
      if (value == null) return null;
      String str = value.toString().trim().toLowerCase();
      return "true".equals(str) || "1".equals(str) || "yes".equals(str) || "on".equals(str);
    });

    register("bool_to_str", value -> {
      if (value == null) return null;
      if (value instanceof Boolean) {
        return ((Boolean) value) ? "true" : "false";
      }
      return value.toString();
    });

    register("bool_to_int", value -> {
      if (value == null) return null;
      if (value instanceof Boolean) {
        return ((Boolean) value) ? 1 : 0;
      }
      String str = value.toString().trim().toLowerCase();
      return ("true".equals(str) || "1".equals(str) || "yes".equals(str)) ? 1 : 0;
    });

    // String operations (using Apache Commons Lang)
    register("uppercase", value -> {
      if (value == null) return null;
      return StringUtils.upperCase(value.toString());
    });

    register("lowercase", value -> {
      if (value == null) return null;
      return StringUtils.lowerCase(value.toString());
    });

    register("trim", value -> {
      if (value == null) return null;
      return StringUtils.trim(value.toString());
    });

    register("trim_to_empty", value -> {
      return StringUtils.trimToEmpty(value == null ? null : value.toString());
    });

    register("trim_to_null", value -> {
      return StringUtils.trimToNull(value == null ? null : value.toString());
    });

    register("capitalize", value -> {
      if (value == null) return null;
      return StringUtils.capitalize(value.toString());
    });

    register("uncapitalize", value -> {
      if (value == null) return null;
      return StringUtils.uncapitalize(value.toString());
    });

    register("swap_case", value -> {
      if (value == null) return null;
      return StringUtils.swapCase(value.toString());
    });

    // Number formatting
    register("format_decimal", value -> {
      if (value == null) return null;
      if (value instanceof Number) {
        double d = ((Number) value).doubleValue();
        // Remove trailing zeros
        if (d == (long) d) {
          return String.valueOf((long) d);
        }
        return String.valueOf(d);
      }
      return value.toString();
    });

    // Null/empty handling
    register("default_empty", value -> {
      return value == null ? "" : value;
    });

    register("default_null", value -> {
      if (value == null) return null;
      String str = value.toString().trim();
      return str.isEmpty() ? null : value;
    });
  }

  /**
   * Register regex-based transformations (requires pattern and replacement as parameters).
   * These are registered as parameterized transformations.
   */
  public void registerRegexTransformation(String name, String pattern, String replacement) {
    register(name, value -> {
      if (value == null) return null;
      String str = value.toString();
      try {
        return str.replaceAll(pattern, replacement);
      } catch (Exception e) {
        log.warn("Regex transformation '{}' failed: {}", name, e.getMessage());
        return value;
      }
    });
  }

  /**
   * Apply a regex replace transformation.
   * 
   * @param value input value
   * @param pattern regex pattern
   * @param replacement replacement string (can use $1, $2, etc. for groups)
   * @return transformed value
   */
  public Object applyRegexReplace(Object value, String pattern, String replacement) {
    if (value == null) return null;
    String str = value.toString();
    try {
      return str.replaceAll(pattern, replacement);
    } catch (Exception e) {
      log.warn("Regex replace failed: pattern='{}', replacement='{}', error={}", 
          pattern, replacement, e.getMessage());
      return value;
    }
  }

  /**
   * Apply a regex extract transformation (extracts first matching group).
   * 
   * @param value input value
   * @param pattern regex pattern with capturing groups
   * @return extracted value, or original if no match
   */
  public Object applyRegexExtract(Object value, String pattern) {
    if (value == null) return null;
    String str = value.toString();
    try {
      Pattern p = Pattern.compile(pattern);
      Matcher m = p.matcher(str);
      if (m.find()) {
        // Return first capturing group, or full match if no groups
        return m.groupCount() > 0 ? m.group(1) : m.group(0);
      }
      return value; // No match, return original
    } catch (Exception e) {
      log.warn("Regex extract failed: pattern='{}', error={}", pattern, e.getMessage());
      return value;
    }
  }

  /**
   * Get list of all registered transformation names.
   */
  public java.util.Set<String> getAvailableTransformations() {
    return new java.util.HashSet<>(transformations.keySet());
  }
}

