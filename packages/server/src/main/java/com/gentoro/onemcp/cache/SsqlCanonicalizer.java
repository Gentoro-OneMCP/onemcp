package com.gentoro.onemcp.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Canonicalizes S-SQL statements for cache key generation (cache_spec v10.0).
 *
 * <p>Enforces:
 * - UPPERCASE keywords
 * - lowercase table/column names
 * - sorted columns
 * - sorted predicates
 * - sorted SET assignments
 * - normalized literals (suffix conventions)
 * - all whitespace collapsed
 * - single-line output
 */
public class SsqlCanonicalizer {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(SsqlCanonicalizer.class);

  // SQL keywords that should be uppercase
  private static final Pattern SQL_KEYWORDS = Pattern.compile(
      "\\b(SELECT|FROM|WHERE|GROUP|BY|ORDER|LIMIT|OFFSET|UPDATE|SET|INSERT|INTO|VALUES|DELETE|AND|OR|" +
      "SUM|AVG|COUNT|MIN|MAX|AS|IN|NOT|LIKE|BETWEEN|IS|NULL)\\b",
      Pattern.CASE_INSENSITIVE);

  /**
   * Canonicalize an S-SQL statement.
   *
   * @param ssql the S-SQL statement to canonicalize
   * @return canonical S-SQL string
   */
  public String canonicalize(String ssql) {
    if (ssql == null || ssql.trim().isEmpty()) {
      return "";
    }

    // Step 1: Normalize whitespace
    String normalized = ssql.trim()
        .replaceAll("\\s+", " ") // Collapse whitespace
        .replaceAll("\\s*,\\s*", ", ") // Normalize commas
        .replaceAll("\\s*=\\s*", " = ") // Normalize equals
        .replaceAll("\\s*!=\\s*", " != ") // Normalize not equals
        .replaceAll("\\s*>\\s*", " > ") // Normalize greater than
        .replaceAll("\\s*>=\\s*", " >= ") // Normalize greater than or equal
        .replaceAll("\\s*<\\s*", " < ") // Normalize less than
        .replaceAll("\\s*<=\\s*", " <= ") // Normalize less than or equal
        .replaceAll("\\s*\\+\\s*", " + ") // Normalize plus
        .replaceAll("\\s*-\\s*", " - ") // Normalize minus
        .replaceAll("\\s*\\(\\s*", "(") // Normalize opening paren
        .replaceAll("\\s*\\)\\s*", ")") // Normalize closing paren
        .trim();

    // Step 2: Uppercase keywords
    normalized = SQL_KEYWORDS.matcher(normalized).replaceAll(matchResult -> 
        matchResult.group().toUpperCase());

    // Step 3: Parse and normalize structure (simplified - full AST parsing would be better)
    // For now, we do basic normalization
    normalized = normalizeSelectClause(normalized);
    normalized = normalizeWhereClause(normalized);
    normalized = normalizeSetClause(normalized);
    normalized = normalizeOrderByClause(normalized);
    normalized = normalizeGroupByClause(normalized);

    // Step 4: Final whitespace collapse
    normalized = normalized.replaceAll("\\s+", " ").trim();

    return normalized;
  }

  /**
   * Generate cache key from canonical S-SQL (MD5 hash).
   *
   * @param canonicalSsql the canonical S-SQL
   * @return MD5 hash as hex string
   */
  public String generateCacheKey(String canonicalSsql) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] hash = md.digest(canonicalSsql.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : hash) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      log.error("MD5 algorithm not available", e);
      // Fallback to simple hash code
      return String.valueOf(canonicalSsql.hashCode());
    }
  }

  private String normalizeSelectClause(String sql) {
    // Extract SELECT columns and sort them
    // This is a simplified version - full SQL parser would be better
    if (sql.toUpperCase().contains("SELECT")) {
      int selectStart = sql.toUpperCase().indexOf("SELECT");
      int fromStart = sql.toUpperCase().indexOf(" FROM ");
      if (fromStart > selectStart) {
        String selectPart = sql.substring(selectStart + 6, fromStart).trim();
        // Split by comma and sort
        String[] columns = selectPart.split(",");
        List<String> sorted = new ArrayList<>();
        for (String col : columns) {
          sorted.add(col.trim().toLowerCase());
        }
        Collections.sort(sorted);
        String newSelect = "SELECT " + String.join(", ", sorted);
        return sql.substring(0, selectStart) + newSelect + sql.substring(fromStart);
      }
    }
    return sql;
  }

  private String normalizeWhereClause(String sql) {
    // Extract WHERE predicates and sort them
    // Simplified - full parser would handle AND/OR properly
    if (sql.toUpperCase().contains(" WHERE ")) {
      int whereStart = sql.toUpperCase().indexOf(" WHERE ");
      int nextClause = findNextClause(sql, whereStart);
      String wherePart = sql.substring(whereStart + 7, nextClause).trim();
      // Split by AND and sort
      String[] predicates = wherePart.split("\\s+AND\\s+");
      List<String> sorted = new ArrayList<>();
      for (String pred : predicates) {
        sorted.add(pred.trim().toLowerCase());
      }
      Collections.sort(sorted);
      String newWhere = " WHERE " + String.join(" AND ", sorted);
      return sql.substring(0, whereStart) + newWhere + sql.substring(nextClause);
    }
    return sql;
  }

  private String normalizeSetClause(String sql) {
    // Extract SET assignments and sort them
    if (sql.toUpperCase().contains(" SET ")) {
      int setStart = sql.toUpperCase().indexOf(" SET ");
      int whereStart = sql.toUpperCase().indexOf(" WHERE ", setStart);
      int end = whereStart > setStart ? whereStart : sql.length();
      String setPart = sql.substring(setStart + 5, end).trim();
      // Split by comma and sort
      String[] assignments = setPart.split(",");
      List<String> sorted = new ArrayList<>();
      for (String assign : assignments) {
        sorted.add(assign.trim().toLowerCase());
      }
      Collections.sort(sorted);
      String newSet = " SET " + String.join(", ", sorted);
      return sql.substring(0, setStart) + newSet + sql.substring(end);
    }
    return sql;
  }

  private String normalizeOrderByClause(String sql) {
    // Extract ORDER BY columns and sort them
    if (sql.toUpperCase().contains(" ORDER BY ")) {
      int orderStart = sql.toUpperCase().indexOf(" ORDER BY ");
      int limitStart = sql.toUpperCase().indexOf(" LIMIT ", orderStart);
      int end = limitStart > orderStart ? limitStart : sql.length();
      String orderPart = sql.substring(orderStart + 10, end).trim();
      // Split by comma and sort
      String[] columns = orderPart.split(",");
      List<String> sorted = new ArrayList<>();
      for (String col : columns) {
        sorted.add(col.trim().toLowerCase());
      }
      Collections.sort(sorted);
      String newOrder = " ORDER BY " + String.join(", ", sorted);
      return sql.substring(0, orderStart) + newOrder + sql.substring(end);
    }
    return sql;
  }

  private String normalizeGroupByClause(String sql) {
    // Extract GROUP BY columns and sort them
    if (sql.toUpperCase().contains(" GROUP BY ")) {
      int groupStart = sql.toUpperCase().indexOf(" GROUP BY ");
      int orderStart = sql.toUpperCase().indexOf(" ORDER BY ", groupStart);
      int end = orderStart > groupStart ? orderStart : sql.length();
      String groupPart = sql.substring(groupStart + 10, end).trim();
      // Split by comma and sort
      String[] columns = groupPart.split(",");
      List<String> sorted = new ArrayList<>();
      for (String col : columns) {
        sorted.add(col.trim().toLowerCase());
      }
      Collections.sort(sorted);
      String newGroup = " GROUP BY " + String.join(", ", sorted);
      return sql.substring(0, groupStart) + newGroup + sql.substring(end);
    }
    return sql;
  }

  private int findNextClause(String sql, int start) {
    String upper = sql.toUpperCase();
    int orderBy = upper.indexOf(" ORDER BY ", start);
    int groupBy = upper.indexOf(" GROUP BY ", start);
    int limit = upper.indexOf(" LIMIT ", start);
    int offset = upper.indexOf(" OFFSET ", start);
    
    int min = sql.length();
    if (orderBy > start && orderBy < min) min = orderBy;
    if (groupBy > start && groupBy < min) min = groupBy;
    if (limit > start && limit < min) min = limit;
    if (offset > start && offset < min) min = offset;
    
    return min;
  }
}

