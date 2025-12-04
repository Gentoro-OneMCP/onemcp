package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON Intermediate Representation (JSON IR) for S-SQL statements.
 * 
 * <p>This structure represents a single S-SQL statement in a structured, 
 * deterministic format that can be rendered to canonical S-SQL.
 * 
 * <p>All literal values are replaced with ? placeholders. Literal values
 * are returned separately in a parallel array.
 */
public class SsqlJsonIr {
  @JsonProperty("operation")
  private String operation; // "select" | "update" | "insert" | "delete"

  @JsonProperty("table")
  private String table;

  @JsonProperty("columns")
  private List<String> columns; // Required for SELECT/INSERT

  @JsonProperty("where")
  private List<List<String>> where; // [["column", "operator", "?"], ...]

  @JsonProperty("group_by")
  private List<String> groupBy;

  @JsonProperty("aggregates")
  private List<List<String>> aggregates; // [["function", "column", "alias"], ...]

  @JsonProperty("order_by")
  private List<String> orderBy;

  @JsonProperty("limit")
  private Integer limit;

  @JsonProperty("offset")
  private Integer offset;

  @JsonProperty("set")
  private List<List<String>> set; // For UPDATE: [["column", "?"], ...]

  @JsonProperty("values")
  private List<String> values; // For INSERT: ["?", "?", ...]

  public SsqlJsonIr() {
    this.columns = new ArrayList<>();
    this.where = new ArrayList<>();
    this.groupBy = new ArrayList<>();
    this.aggregates = new ArrayList<>();
    this.orderBy = new ArrayList<>();
    this.set = new ArrayList<>();
    this.values = new ArrayList<>();
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation != null ? operation.toLowerCase() : null;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<String> getColumns() {
    return columns != null ? columns : new ArrayList<>();
  }

  public void setColumns(List<String> columns) {
    this.columns = columns != null ? columns : new ArrayList<>();
  }

  public List<List<String>> getWhere() {
    return where != null ? where : new ArrayList<>();
  }

  public void setWhere(List<List<String>> where) {
    this.where = where != null ? where : new ArrayList<>();
  }

  public List<String> getGroupBy() {
    return groupBy != null ? groupBy : new ArrayList<>();
  }

  public void setGroupBy(List<String> groupBy) {
    this.groupBy = groupBy != null ? groupBy : new ArrayList<>();
  }

  public List<List<String>> getAggregates() {
    return aggregates != null ? aggregates : new ArrayList<>();
  }

  public void setAggregates(List<List<String>> aggregates) {
    this.aggregates = aggregates != null ? aggregates : new ArrayList<>();
  }

  public List<String> getOrderBy() {
    return orderBy != null ? orderBy : new ArrayList<>();
  }

  public void setOrderBy(List<String> orderBy) {
    this.orderBy = orderBy != null ? orderBy : new ArrayList<>();
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  public List<List<String>> getSet() {
    return set != null ? set : new ArrayList<>();
  }

  public void setSet(List<List<String>> set) {
    this.set = set != null ? set : new ArrayList<>();
  }

  public List<String> getValues() {
    return values != null ? values : new ArrayList<>();
  }

  public void setValues(List<String> values) {
    this.values = values != null ? values : new ArrayList<>();
  }

  /**
   * Validate that the JSON IR is well-formed.
   * 
   * @throws IllegalArgumentException if validation fails
   */
  public void validate() {
    if (operation == null || operation.trim().isEmpty()) {
      throw new IllegalArgumentException("JSON IR operation is required");
    }
    
    String op = operation.toLowerCase();
    if (!op.equals("select") && !op.equals("update") && 
        !op.equals("insert") && !op.equals("delete")) {
      throw new IllegalArgumentException(
          "JSON IR operation must be one of: select, update, insert, delete");
    }
    
    if (table == null || table.trim().isEmpty()) {
      throw new IllegalArgumentException("JSON IR table is required");
    }
    
    // SELECT and INSERT require columns
    if ((op.equals("select") || op.equals("insert")) && 
        (columns == null || columns.isEmpty())) {
      throw new IllegalArgumentException(
          "JSON IR columns are required for " + op + " operation");
    }
    
    // UPDATE requires set
    if (op.equals("update") && (set == null || set.isEmpty())) {
      throw new IllegalArgumentException("JSON IR set is required for update operation");
    }
    
    // INSERT requires values
    if (op.equals("insert") && (values == null || values.isEmpty())) {
      throw new IllegalArgumentException("JSON IR values are required for insert operation");
    }
  }
}

