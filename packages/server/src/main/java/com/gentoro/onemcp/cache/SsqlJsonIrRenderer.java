package com.gentoro.onemcp.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Renders JSON IR to canonical S-SQL.
 * 
 * <p>This renderer is fully deterministic and produces canonical S-SQL
 * that is identical for semantically equivalent JSON IR.
 * 
 * <p>Canonicalization rules:
 * - Keywords UPPERCASE
 * - Table/column names lowercase
 * - WHERE predicates sorted lexicographically by column
 * - ORDER BY sorted alphabetically
 * - GROUP BY sorted alphabetically
 * - Aggregates sorted by alias or column
 * - Columns sorted alphabetically
 * - Entire statement flattened to a single line
 */
public class SsqlJsonIrRenderer {
  
  /**
   * Render JSON IR to canonical S-SQL.
   * 
   * @param jsonIr the JSON IR to render
   * @return canonical S-SQL string (single line)
   * @throws IllegalArgumentException if JSON IR is invalid
   */
  public String render(SsqlJsonIr jsonIr) {
    if (jsonIr == null) {
      throw new IllegalArgumentException("JSON IR cannot be null");
    }
    
    jsonIr.validate();
    
    String operation = jsonIr.getOperation().toLowerCase();
    
    switch (operation) {
      case "select":
        return renderSelect(jsonIr);
      case "update":
        return renderUpdate(jsonIr);
      case "insert":
        return renderInsert(jsonIr);
      case "delete":
        return renderDelete(jsonIr);
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }
  }
  
  private String renderSelect(SsqlJsonIr jsonIr) {
    StringBuilder sb = new StringBuilder();
    
    // SELECT clause
    sb.append("SELECT ");
    
    // Determine what to select: aggregates or columns
    List<List<String>> aggregates = jsonIr.getAggregates();
    List<String> columns = jsonIr.getColumns();
    
    if (!aggregates.isEmpty()) {
      // Render aggregates
      List<String> sortedAggregates = new ArrayList<>();
      for (List<String> agg : aggregates) {
        if (agg.size() >= 2) {
          String func = agg.get(0).toUpperCase();
          String col = agg.get(1).toLowerCase();
          String alias = agg.size() >= 3 ? agg.get(2).toLowerCase() : col;
          sortedAggregates.add(func + "(" + col + ") AS " + alias);
        }
      }
      Collections.sort(sortedAggregates);
      sb.append(String.join(", ", sortedAggregates));
    } else {
      // Render columns (sorted)
      List<String> sortedColumns = new ArrayList<>(columns);
      Collections.sort(sortedColumns);
      for (int i = 0; i < sortedColumns.size(); i++) {
        sortedColumns.set(i, sortedColumns.get(i).toLowerCase());
      }
      sb.append(String.join(", ", sortedColumns));
    }
    
    // FROM clause
    sb.append(" FROM ").append(jsonIr.getTable().toLowerCase());
    
    // WHERE clause
    List<List<String>> where = jsonIr.getWhere();
    if (!where.isEmpty()) {
      sb.append(" WHERE ");
      List<String> predicates = new ArrayList<>();
      for (List<String> pred : where) {
        if (pred.size() >= 3) {
          String col = pred.get(0).toLowerCase();
          String op = pred.get(1).toUpperCase();
          String val = pred.get(2); // Should be "?" (for IN, val is still "?")
          // Handle IN operator specially - render as "column IN (?)"
          if ("IN".equals(op)) {
            predicates.add(col + " IN (" + val + ")");
          } else {
            predicates.add(col + " " + op + " " + val);
          }
        }
      }
      // Sort predicates by column name
      Collections.sort(predicates);
      sb.append(String.join(" AND ", predicates));
    }
    
    // GROUP BY clause
    List<String> groupBy = jsonIr.getGroupBy();
    if (!groupBy.isEmpty()) {
      sb.append(" GROUP BY ");
      List<String> sortedGroupBy = new ArrayList<>(groupBy);
      Collections.sort(sortedGroupBy);
      for (int i = 0; i < sortedGroupBy.size(); i++) {
        sortedGroupBy.set(i, sortedGroupBy.get(i).toLowerCase());
      }
      sb.append(String.join(", ", sortedGroupBy));
    }
    
    // ORDER BY clause
    List<String> orderBy = jsonIr.getOrderBy();
    if (!orderBy.isEmpty()) {
      sb.append(" ORDER BY ");
      List<String> sortedOrderBy = new ArrayList<>(orderBy);
      Collections.sort(sortedOrderBy);
      for (int i = 0; i < sortedOrderBy.size(); i++) {
        sortedOrderBy.set(i, sortedOrderBy.get(i).toLowerCase());
      }
      sb.append(String.join(", ", sortedOrderBy));
    }
    
    // LIMIT clause
    if (jsonIr.getLimit() != null) {
      sb.append(" LIMIT ").append(jsonIr.getLimit());
    }
    
    // OFFSET clause
    if (jsonIr.getOffset() != null) {
      sb.append(" OFFSET ").append(jsonIr.getOffset());
    }
    
    return sb.toString();
  }
  
  private String renderUpdate(SsqlJsonIr jsonIr) {
    StringBuilder sb = new StringBuilder();
    
    // UPDATE clause
    sb.append("UPDATE ").append(jsonIr.getTable().toLowerCase());
    
    // SET clause
    List<List<String>> set = jsonIr.getSet();
    if (!set.isEmpty()) {
      sb.append(" SET ");
      List<String> assignments = new ArrayList<>();
      for (List<String> assignment : set) {
        if (assignment.size() >= 2) {
          String col = assignment.get(0).toLowerCase();
          String val = assignment.get(1); // Should be "?"
          assignments.add(col + " = " + val);
        }
      }
      // Sort assignments by column name
      Collections.sort(assignments);
      sb.append(String.join(", ", assignments));
    }
    
    // WHERE clause
    List<List<String>> where = jsonIr.getWhere();
    if (!where.isEmpty()) {
      sb.append(" WHERE ");
      List<String> predicates = new ArrayList<>();
      for (List<String> pred : where) {
        if (pred.size() >= 3) {
          String col = pred.get(0).toLowerCase();
          String op = pred.get(1).toUpperCase();
          String val = pred.get(2);
          predicates.add(col + " " + op + " " + val);
        }
      }
      Collections.sort(predicates);
      sb.append(String.join(" AND ", predicates));
    }
    
    return sb.toString();
  }
  
  private String renderInsert(SsqlJsonIr jsonIr) {
    StringBuilder sb = new StringBuilder();
    
    // INSERT INTO clause
    sb.append("INSERT INTO ").append(jsonIr.getTable().toLowerCase());
    
    // Columns
    List<String> columns = jsonIr.getColumns();
    if (!columns.isEmpty()) {
      sb.append(" (");
      List<String> sortedColumns = new ArrayList<>(columns);
      Collections.sort(sortedColumns);
      for (int i = 0; i < sortedColumns.size(); i++) {
        sortedColumns.set(i, sortedColumns.get(i).toLowerCase());
      }
      sb.append(String.join(", ", sortedColumns));
      sb.append(")");
    }
    
    // VALUES clause
    List<String> values = jsonIr.getValues();
    if (!values.isEmpty()) {
      sb.append(" VALUES (");
      sb.append(String.join(", ", values));
      sb.append(")");
    }
    
    return sb.toString();
  }
  
  private String renderDelete(SsqlJsonIr jsonIr) {
    StringBuilder sb = new StringBuilder();
    
    // DELETE FROM clause
    sb.append("DELETE FROM ").append(jsonIr.getTable().toLowerCase());
    
    // WHERE clause
    List<List<String>> where = jsonIr.getWhere();
    if (!where.isEmpty()) {
      sb.append(" WHERE ");
      List<String> predicates = new ArrayList<>();
      for (List<String> pred : where) {
        if (pred.size() >= 3) {
          String col = pred.get(0).toLowerCase();
          String op = pred.get(1).toUpperCase();
          String val = pred.get(2);
          predicates.add(col + " " + op + " " + val);
        }
      }
      Collections.sort(predicates);
      sb.append(String.join(" AND ", predicates));
    }
    
    return sb.toString();
  }
}

