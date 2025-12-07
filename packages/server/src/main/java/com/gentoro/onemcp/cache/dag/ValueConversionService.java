package com.gentoro.onemcp.cache.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.cache.dag.conversion.ValueConverter;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts PS conceptual values to API format values before DAG execution.
 * 
 * <p>This service analyzes the DAG plan to find ConvertValue nodes and extracts
 * conversion requirements. It then converts PS values upfront, so the DAG executor
 * can use the converted values directly instead of needing ConvertValue nodes.
 */
public class ValueConversionService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ValueConversionService.class);

  private final ValueConverter valueConverter;

  public ValueConversionService() {
    this.valueConverter = new ValueConverter();
  }

  /**
   * Convert PS values to API format based on conversion requirements in the DAG plan.
   * 
   * @param dagPlan the DAG plan (may contain ConvertValue nodes)
   * @param psValues original PS values (conceptual DTN strings)
   * @return converted values (API format), with same keys as psValues
   */
  public Map<String, String> convertValues(DagPlan dagPlan, Map<String, String> psValues) {
    if (psValues == null || psValues.isEmpty()) {
      return new HashMap<>();
    }

    // Extract conversion requirements from ConvertValue nodes
    Map<String, ConversionRequirement> conversions = extractConversionRequirements(dagPlan);
    
    if (conversions.isEmpty()) {
      // No conversions needed - return original values
      return new HashMap<>(psValues);
    }

    // Convert each PS value based on its conversion requirement
    Map<String, String> converted = new HashMap<>();
    for (Map.Entry<String, String> entry : psValues.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      
      ConversionRequirement req = conversions.get(key);
      if (req != null) {
        // Convert the value
        Object convertedValue = valueConverter.convert(
            req.conceptualFieldKind, 
            req.targetFormat, 
            value);
        converted.put(key, String.valueOf(convertedValue));
        log.debug("Converted {}: {} -> {} ({} -> {})", 
            key, value, convertedValue, req.conceptualFieldKind, req.targetFormat);
      } else {
        // No conversion needed - use original value
        converted.put(key, value);
      }
    }

    return converted;
  }

  /**
   * Extract conversion requirements from ConvertValue nodes in the DAG plan.
   * 
   * @param dagPlan the DAG plan
   * @return map from PS value key (e.g., "v1") to conversion requirement
   */
  private Map<String, ConversionRequirement> extractConversionRequirements(DagPlan dagPlan) {
    Map<String, ConversionRequirement> conversions = new HashMap<>();
    
    for (DagNode node : dagPlan.getNodes()) {
      if ("ConvertValue".equals(node.getType())) {
        ObjectNode config = node.getConfig();
        
        // Get the value reference (e.g., "$._initial.v1")
        String valueRef = config.has("value") ? config.get("value").asText() : null;
        if (valueRef != null && valueRef.startsWith("$._initial.")) {
          // Extract the PS value key (e.g., "v1" from "$._initial.v1")
          String psValueKey = valueRef.substring("$._initial.".length());
          
          // Get conversion parameters
          String conceptualFieldKind = config.has("conceptualFieldKind") 
              ? config.get("conceptualFieldKind").asText() : null;
          String targetFormat = config.has("targetFormat") 
              ? config.get("targetFormat").asText() : null;
          
          conversions.put(psValueKey, new ConversionRequirement(conceptualFieldKind, targetFormat));
          log.debug("Found conversion requirement for {}: {} -> {}", 
              psValueKey, conceptualFieldKind, targetFormat);
        }
      }
    }
    
    return conversions;
  }

  /**
   * Represents a conversion requirement for a PS value.
   */
  private static class ConversionRequirement {
    final String conceptualFieldKind;
    final String targetFormat;

    ConversionRequirement(String conceptualFieldKind, String targetFormat) {
      this.conceptualFieldKind = conceptualFieldKind;
      this.targetFormat = targetFormat;
    }
  }
}

