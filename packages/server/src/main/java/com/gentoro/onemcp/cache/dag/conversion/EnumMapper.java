package com.gentoro.onemcp.cache.dag.conversion;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps conceptual enum values to backend enum values.
 * 
 * <p>Implements MapEnum node conversions.
 * Uses a registry of enum mappings that can be populated from API metadata.
 */
public class EnumMapper {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(EnumMapper.class);

  // Registry of enum mappings: (conceptualFieldKind, targetFormat) -> (conceptualValue -> apiValue)
  private final Map<String, Map<String, String>> enumMappings = new HashMap<>();

  /**
   * Register an enum mapping.
   * 
   * @param conceptualFieldKind conceptual field type (e.g., "status")
   * @param targetFormat target format identifier (e.g., "status_enum")
   * @param conceptualValue conceptual value (e.g., "archived")
   * @param apiValue API value (e.g., "STATUS_ARCHIVED")
   */
  public void registerMapping(String conceptualFieldKind, String targetFormat, 
      String conceptualValue, String apiValue) {
    String key = conceptualFieldKind + ":" + targetFormat;
    enumMappings.computeIfAbsent(key, k -> new HashMap<>()).put(conceptualValue, apiValue);
  }

  /**
   * Map a conceptual enum value to an API enum value.
   * 
   * @param conceptualFieldKind conceptual field type
   * @param targetFormat target format identifier
   * @param value conceptual value
   * @return API value, or original value if no mapping found
   */
  public String map(String conceptualFieldKind, String targetFormat, String value) {
    String key = conceptualFieldKind + ":" + targetFormat;
    Map<String, String> mapping = enumMappings.get(key);
    
    if (mapping == null) {
      log.warn("No enum mapping found for {}:{}", conceptualFieldKind, targetFormat);
      return value; // Fallback to original value
    }
    
    String mapped = mapping.get(value);
    if (mapped == null) {
      log.warn("No enum mapping found for {}:{} -> {}", conceptualFieldKind, targetFormat, value);
      return value; // Fallback to original value
    }
    
    return mapped;
  }
}

