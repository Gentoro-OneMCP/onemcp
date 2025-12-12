package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Labeled placeholder for values in expression trees (cache_spec v16.0).
 * 
 * <p>Single value: { "label": "v1" }
 * Multi-value: { "labels": ["v1", "v2", "v3"] }
 */
public sealed interface Placeholder {
  
  /**
   * Single-value placeholder.
   */
  record SingleLabel(
      @JsonProperty("label") String label
  ) implements Placeholder {}
  
  /**
   * Multi-value placeholder (for in/not_in operators).
   */
  record MultiLabel(
      @JsonProperty("labels") List<String> labels
  ) implements Placeholder {}
}





