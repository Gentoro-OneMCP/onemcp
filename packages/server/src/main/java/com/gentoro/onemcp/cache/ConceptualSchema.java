package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Conceptual schema for S-SQL normalization (cache_spec v10.0).
 *
 * <p>Represents API as conceptual tables with conceptual columns.
 * The conceptual schema is produced by the Transducer and provides
 * semantic meaning, not literal API structure.
 *
 * <p>Provides lookup methods:
 * - get_columns(table) → conceptual columns for this table
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConceptualSchema {
  @JsonProperty("tables")
  private Map<String, TableDefinition> tables = new HashMap<>();

  public ConceptualSchema() {}

  public Map<String, TableDefinition> getTables() {
    return tables;
  }

  public void setTables(Map<String, TableDefinition> tables) {
    this.tables = tables != null ? tables : new HashMap<>();
  }

  /** Get table definition by name. */
  public TableDefinition getTable(String tableName) {
    return tables.get(tableName);
  }

  /** Check if a table exists. */
  public boolean hasTable(String tableName) {
    return tables.containsKey(tableName);
  }

  /** Get columns for a table (get_columns(table) API). */
  public List<String> getColumns(String tableName) {
    TableDefinition table = tables.get(tableName);
    return table != null ? table.getColumns() : java.util.Collections.emptyList();
  }

  /** Check if a column exists in a table. */
  public boolean hasColumn(String tableName, String columnName) {
    TableDefinition table = tables.get(tableName);
    return table != null && table.getColumns().contains(columnName);
  }

  /**
   * Table definition with conceptual columns.
   * Conceptual columns reflect meaning, not literal API fields.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableDefinition {
    @JsonProperty("columns")
    private List<String> columns;

    public TableDefinition() {}

    public TableDefinition(List<String> columns) {
      this.columns = columns != null ? columns : new java.util.ArrayList<>();
    }

    public List<String> getColumns() {
      return columns;
    }

    public void setColumns(List<String> columns) {
      this.columns = columns != null ? columns : new java.util.ArrayList<>();
    }
  }
}



