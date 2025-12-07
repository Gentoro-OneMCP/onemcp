package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Canonicalizes Prompt Schema (PS) for deterministic cache key generation.
 * 
 * <p>Cache key is based ONLY on the "filter structure" to maximize cache hits:
 * - operation, table, where, group_by
 * 
 * <p>Excluded from cache key (allows flexible output):
 * - fields (output columns/aggregates)
 * - order_by (sorting)
 * - limit/offset (pagination)
 * - values (actual literals)
 * 
 * <p>Canonicalization rules:
 * - Lowercase all identifiers (operation, column names, aggregate names)
 * - Apply Value-Set Generalization (VSG) to expression tree
 * - Canonicalize expression tree by sorting boolean children
 * - Collapse trivial AND/OR nodes
 * - Generate MD5 hash of canonical JSON structure
 */
public class PromptSchemaCanonicalizer {
  
  /**
   * Helper record for VSG label reordering when values are sorted.
   */
  private record LabelValuePair(String label, Object value) {}
  
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(PromptSchemaCanonicalizer.class);

  /**
   * Canonicalize a Prompt Schema.
   * 
   * @param ps the prompt schema to canonicalize
   * @return canonicalized prompt schema (new instance)
   */
  public PromptSchema canonicalize(PromptSchema ps) {
    if (ps == null) {
      return null;
    }
    
    PromptSchema canonical = new PromptSchema();
    
    // Lowercase operation
    canonical.setOperation(ps.getOperation() != null ? ps.getOperation().toLowerCase() : null);
    canonical.setTable(ps.getTable() != null ? ps.getTable().toLowerCase() : null);
    
    // Canonicalize fields (sort, lowercase)
    canonical.setFields(canonicalizeFields(ps.getFields()));
    
    // Canonicalize WHERE expression tree (apply VSG first, then normalize)
    canonical.setWhere(canonicalizeExpressionTree(ps.getWhere(), ps.getValues() != null ? ps.getValues() : new HashMap<>()));
    
    // Sort and lowercase group_by
    canonical.setGroupBy(canonicalizeStringList(ps.getGroupBy()));
    
    // Sort and lowercase order_by
    canonical.setOrderBy(canonicalizeStringList(ps.getOrderBy()));
    
    // Copy limit and offset as-is
    canonical.setLimit(ps.getLimit());
    canonical.setOffset(ps.getOffset());
    
    // Values are NOT included in canonical form (excluded from cache key)
    // But we keep them in the PS for the planner
    // Values are already a Map, just copy it
    canonical.setValues(ps.getValues() != null ? new HashMap<>(ps.getValues()) : new HashMap<>());
    
    // Extract and sort columns from all references
    canonical.setColumns(extractAndSortColumns(ps));
    
    // Note and original_prompt are not part of canonical structure
    canonical.setNote(ps.getNote());
    canonical.setCurrentTime(ps.getCurrentTime());
    canonical.setOriginalPrompt(ps.getOriginalPrompt());
    
    return canonical;
  }
  
  /**
   * Canonicalize fields list.
   * Fields can be strings or aggregate objects like { "agg": "sum", "column": "amount" }
   */
  private List<Object> canonicalizeFields(List<Object> fields) {
    if (fields == null || fields.isEmpty()) {
      return new ArrayList<>();
    }
    
    List<Object> canonical = new ArrayList<>();
    for (Object field : fields) {
      if (field instanceof String) {
        canonical.add(((String) field).toLowerCase());
      } else if (field instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> agg = (Map<String, Object>) field;
        Map<String, Object> canonicalAgg = new HashMap<>();
        if (agg.containsKey("agg")) {
          canonicalAgg.put("agg", agg.get("agg").toString().toLowerCase());
        }
        if (agg.containsKey("column")) {
          canonicalAgg.put("column", agg.get("column").toString().toLowerCase());
        }
        canonical.add(canonicalAgg);
      } else {
        canonical.add(field);
      }
    }
    
    // Sort fields: simple strings first, then aggregates
    canonical.sort((a, b) -> {
      boolean aIsString = a instanceof String;
      boolean bIsString = b instanceof String;
      if (aIsString && bIsString) {
        return ((String) a).compareTo((String) b);
      } else if (aIsString) {
        return -1;
      } else if (bIsString) {
        return 1;
      } else {
        // Both are aggregates - sort by JSON representation
        try {
          String aJson = JacksonUtility.getJsonMapper().writeValueAsString(a);
          String bJson = JacksonUtility.getJsonMapper().writeValueAsString(b);
          return aJson.compareTo(bJson);
        } catch (Exception e) {
          return 0;
        }
      }
    });
    
    return canonical;
  }
  
  /**
   * Canonicalize expression tree with Value-Set Generalization (VSG).
   * 
   * @param expr the expression tree to canonicalize
   * @param values the values map (may be modified by VSG)
   * @return canonicalized expression tree
   */
  private ExpressionTree canonicalizeExpressionTree(ExpressionTree expr, Map<String, Object> values) {
    if (expr == null) {
      return null;
    }
    
    // First apply VSG transformations
    ExpressionTree vsgApplied = applyValueSetGeneralization(expr, values);
    
    // Then normalize (lowercase, sort, collapse)
    return normalizeExpressionTree(vsgApplied);
  }
  
  /**
   * Apply Value-Set Generalization (VSG) to expression tree.
   * 
   * VSG rules:
   * - Single-value equality (=) → convert to `in` with single value
   * - Multiple equality comparisons on same column with OR → convert to `in` with all values
   * - Single-value inequality (!=) → convert to `not_in` with single value
   * - Multiple inequality comparisons on same column with OR → convert to `not_in` with all values
   * - Sort values lexically (case-insensitive) and reorder labels to match
   */
  private ExpressionTree applyValueSetGeneralization(ExpressionTree expr, Map<String, Object> values) {
    if (expr == null) {
      return null;
    }
    
    if (expr instanceof ExpressionTree.ComparisonNode comp) {
      // Single-value equality/inequality: convert to in/not_in
      String op = comp.op().toLowerCase();
      if (op.equals("=")) {
        // Convert = to in with single value
        return new ExpressionTree.ComparisonNode("in", comp.column(), comp.value());
      } else if (op.equals("!=")) {
        // Convert != to not_in with single value
        return new ExpressionTree.ComparisonNode("not_in", comp.column(), comp.value());
      }
      // Other operators stay as-is
      return comp;
    } else if (expr instanceof ExpressionTree.BooleanNode bool) {
      String boolOp = bool.op().toLowerCase();
      
      // Apply VSG recursively to children first
      List<ExpressionTree> processedChildren = new ArrayList<>();
      for (ExpressionTree child : bool.conditions()) {
        ExpressionTree processed = applyValueSetGeneralization(child, values);
        if (processed != null) {
          processedChildren.add(processed);
        }
      }
      
      // VSG: Look for OR nodes with multiple equality comparisons on the same column
      if (boolOp.equals("or") && processedChildren.size() > 1) {
        // Group comparisons by column and operator type
        Map<String, List<ExpressionTree.ComparisonNode>> equalityGroups = new HashMap<>();
        Map<String, List<ExpressionTree.ComparisonNode>> inequalityGroups = new HashMap<>();
        List<ExpressionTree> otherNodes = new ArrayList<>();
        
        for (ExpressionTree child : processedChildren) {
          if (child instanceof ExpressionTree.ComparisonNode comp) {
            String col = comp.column().toLowerCase();
            String op = comp.op().toLowerCase();
            if (op.equals("=") || op.equals("in")) {
              equalityGroups.computeIfAbsent(col, k -> new ArrayList<>()).add(comp);
            } else if (op.equals("!=") || op.equals("not_in")) {
              inequalityGroups.computeIfAbsent(col, k -> new ArrayList<>()).add(comp);
            } else {
              otherNodes.add(comp);
            }
          } else {
            otherNodes.add(child);
          }
        }
        
        // Apply VSG: merge equality groups into single `in` operator
        List<ExpressionTree> merged = new ArrayList<>(otherNodes);
        for (Map.Entry<String, List<ExpressionTree.ComparisonNode>> entry : equalityGroups.entrySet()) {
          String col = entry.getKey();
          List<ExpressionTree.ComparisonNode> comps = entry.getValue();
          if (comps.size() > 1 || (comps.size() == 1 && comps.get(0).op().equals("="))) {
            // Extract labels and values, then sort values and reorder labels
            List<String> labels = new ArrayList<>();
            List<Object> labelValues = new ArrayList<>();
            for (ExpressionTree.ComparisonNode comp : comps) {
              if (comp.value() instanceof Placeholder.SingleLabel single) {
                labels.add(single.label());
                labelValues.add(values.get(single.label()));
              } else if (comp.value() instanceof Placeholder.MultiLabel multi) {
                for (String label : multi.labels()) {
                  labels.add(label);
                  labelValues.add(values.get(label));
                }
              }
            }
            
            // Sort values lexically (case-insensitive) and reorder labels to match
            List<LabelValuePair> pairs = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
              pairs.add(new LabelValuePair(labels.get(i), labelValues.get(i)));
            }
            pairs.sort(Comparator.comparing(pair -> 
              pair.value() != null ? pair.value().toString().toLowerCase() : "", 
              String.CASE_INSENSITIVE_ORDER));
            
            // Extract sorted labels
            List<String> sortedLabels = new ArrayList<>();
            for (LabelValuePair pair : pairs) {
              sortedLabels.add(pair.label());
            }
            
            merged.add(new ExpressionTree.ComparisonNode("in", col, new Placeholder.MultiLabel(sortedLabels)));
          } else {
            // Already an `in` operator, keep as-is
            merged.addAll(comps);
          }
        }
        
        // Apply VSG: merge inequality groups into single `not_in` operator
        for (Map.Entry<String, List<ExpressionTree.ComparisonNode>> entry : inequalityGroups.entrySet()) {
          String col = entry.getKey();
          List<ExpressionTree.ComparisonNode> comps = entry.getValue();
          if (comps.size() > 1 || (comps.size() == 1 && comps.get(0).op().equals("!="))) {
            // Extract labels and values, then sort values and reorder labels
            List<String> labels = new ArrayList<>();
            List<Object> labelValues = new ArrayList<>();
            for (ExpressionTree.ComparisonNode comp : comps) {
              if (comp.value() instanceof Placeholder.SingleLabel single) {
                labels.add(single.label());
                labelValues.add(values.get(single.label()));
              } else if (comp.value() instanceof Placeholder.MultiLabel multi) {
                for (String label : multi.labels()) {
                  labels.add(label);
                  labelValues.add(values.get(label));
                }
              }
            }
            
            // Sort values lexically (case-insensitive) and reorder labels to match
            List<LabelValuePair> pairs = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
              pairs.add(new LabelValuePair(labels.get(i), labelValues.get(i)));
            }
            pairs.sort(Comparator.comparing(pair -> 
              pair.value() != null ? pair.value().toString().toLowerCase() : "", 
              String.CASE_INSENSITIVE_ORDER));
            
            // Extract sorted labels
            List<String> sortedLabels = new ArrayList<>();
            for (LabelValuePair pair : pairs) {
              sortedLabels.add(pair.label());
            }
            
            merged.add(new ExpressionTree.ComparisonNode("not_in", col, new Placeholder.MultiLabel(sortedLabels)));
          } else {
            // Already a `not_in` operator, keep as-is
            merged.addAll(comps);
          }
        }
        
        // If we merged anything, return the merged structure
        if (merged.size() < processedChildren.size()) {
          // Reconstruct with merged nodes (may need to wrap in AND if mixed with other nodes)
          if (merged.size() == 1) {
            return merged.get(0);
          } else if (merged.size() > 1) {
            return new ExpressionTree.BooleanNode("or", merged);
          }
        }
      }
      
      // No VSG transformation applied, return with processed children
      return new ExpressionTree.BooleanNode(boolOp, processedChildren);
    }
    
    return expr;
  }
  
  /**
   * Normalize expression tree (lowercase, sort, collapse).
   * This is called after VSG has been applied.
   */
  private ExpressionTree normalizeExpressionTree(ExpressionTree expr) {
    if (expr == null) {
      return null;
    }
    
    if (expr instanceof ExpressionTree.ComparisonNode comp) {
      // Lowercase operator and column name, keep placeholder as-is
      return new ExpressionTree.ComparisonNode(
          comp.op().toLowerCase(),
          comp.column().toLowerCase(),
          comp.value() // Placeholder object
      );
    } else if (expr instanceof ExpressionTree.BooleanNode bool) {
      // Recursively normalize children
      List<ExpressionTree> normalizedConditions = new ArrayList<>();
      for (ExpressionTree child : bool.conditions()) {
        ExpressionTree normalizedChild = normalizeExpressionTree(child);
        if (normalizedChild != null) {
          normalizedConditions.add(normalizedChild);
        }
      }
      
      // Collapse single-element AND/OR nodes
      if (normalizedConditions.size() == 1) {
        return normalizedConditions.get(0);
      }
      
      // Sort conditions for determinism
      normalizedConditions.sort(Comparator.comparing(this::expressionTreeToString));
      
      return new ExpressionTree.BooleanNode(
          bool.op().toLowerCase(),
          normalizedConditions
      );
    }
    
    return expr;
  }
  
  /**
   * Convert expression tree to string for sorting.
   */
  private String expressionTreeToString(ExpressionTree expr) {
    if (expr instanceof ExpressionTree.ComparisonNode comp) {
      return comp.column() + ":" + comp.op();
    } else if (expr instanceof ExpressionTree.BooleanNode bool) {
      return bool.op() + ":" + bool.conditions().size();
    }
    return "";
  }
  
  // Note: sortValues removed - values are now a Map and don't need sorting
  // VSG will handle value sorting and label reordering
  
  /**
   * Canonicalize string list (sort and lowercase).
   */
  private List<String> canonicalizeStringList(List<String> list) {
    if (list == null || list.isEmpty()) {
      return new ArrayList<>();
    }
    
    List<String> canonical = new ArrayList<>();
    for (String item : list) {
      canonical.add(item.toLowerCase());
    }
    Collections.sort(canonical);
    return canonical;
  }
  
  /**
   * Extract all columns referenced in the PS and sort them.
   */
  private List<String> extractAndSortColumns(PromptSchema ps) {
    List<String> columns = new ArrayList<>();
    
    // From fields
    if (ps.getFields() != null) {
      for (Object field : ps.getFields()) {
        if (field instanceof String) {
          columns.add(((String) field).toLowerCase());
        } else if (field instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> agg = (Map<String, Object>) field;
          if (agg.containsKey("column")) {
            columns.add(agg.get("column").toString().toLowerCase());
          }
        }
      }
    }
    
    // From WHERE expression tree
    extractColumnsFromExpressionTree(ps.getWhere(), columns);
    
    // From group_by
    if (ps.getGroupBy() != null) {
      for (String col : ps.getGroupBy()) {
        columns.add(col.toLowerCase());
      }
    }
    
    // From order_by
    if (ps.getOrderBy() != null) {
      for (String col : ps.getOrderBy()) {
        columns.add(col.toLowerCase());
      }
    }
    
    // Remove duplicates and sort
    List<String> unique = new ArrayList<>(new java.util.HashSet<>(columns));
    Collections.sort(unique);
    return unique;
  }
  
  /**
   * Recursively extract columns from expression tree.
   */
  private void extractColumnsFromExpressionTree(ExpressionTree expr, List<String> columns) {
    if (expr == null) {
      return;
    }
    
    if (expr instanceof ExpressionTree.ComparisonNode comp) {
      columns.add(comp.column().toLowerCase());
    } else if (expr instanceof ExpressionTree.BooleanNode bool) {
      for (ExpressionTree child : bool.conditions()) {
        extractColumnsFromExpressionTree(child, columns);
      }
    }
  }
  
  /**
   * Generate cache key from canonical structure.
   * 
   * <p>Cache key is based ONLY on "filter structure" to maximize cache hits:
   * - operation, table, where, group_by
   * 
   * <p>Excluded from cache key (allows flexible output):
   * - fields (output columns/aggregates) - passed to plan at runtime
   * - order_by (sorting) - applied post-query
   * - limit/offset (pagination) - applied post-query
   * - values (actual literals) - passed to plan at runtime
   * 
   * @param ps the canonicalized prompt schema
   * @return MD5 hash of canonical filter structure
   */
  public String generateCacheKey(PromptSchema ps) {
    if (ps == null) {
      return null;
    }
    
    try {
      // Create a minimal structure for cache key - ONLY filter structure
      PromptSchema keyStructure = new PromptSchema();
      keyStructure.setOperation(ps.getOperation());
      keyStructure.setTable(ps.getTable());
      keyStructure.setWhere(ps.getWhere());
      keyStructure.setGroupBy(ps.getGroupBy());
      
      // EXCLUDED from cache key (allows flexible output):
      keyStructure.setFields(null);      // Output fields - dynamic
      keyStructure.setOrderBy(null);     // Sorting - applied post-query
      keyStructure.setLimit(null);       // Pagination - applied post-query
      keyStructure.setOffset(null);      // Pagination - applied post-query
      keyStructure.setValues(null);      // Literal values - passed at runtime
      keyStructure.setColumns(null);     // Derived from fields
      keyStructure.setCacheKey(null);
      keyStructure.setCurrentTime(null);
      keyStructure.setNote(null);
      keyStructure.setOriginalPrompt(null);
      
      // Serialize to JSON deterministically (compact, not pretty-printed for consistency)
      ObjectMapper mapper = JacksonUtility.getJsonMapper();
      String json = mapper.writeValueAsString(keyStructure);
      
      // Generate MD5 hash
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] hashBytes = md.digest(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      
      // Convert to hex string
      StringBuilder hexString = new StringBuilder();
      for (byte b : hashBytes) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      
      return hexString.toString();
    } catch (Exception e) {
      log.error("Failed to generate cache key", e);
      return null;
    }
  }
}

