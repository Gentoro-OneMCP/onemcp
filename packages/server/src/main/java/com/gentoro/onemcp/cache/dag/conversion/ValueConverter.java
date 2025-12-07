package com.gentoro.onemcp.cache.dag.conversion;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Converts conceptual DTN values to API-required formats.
 * 
 * <p>Implements ConvertValue node conversions based on:
 * - conceptualFieldKind: the conceptual field type (e.g., date_yyyy_mm_dd)
 * - targetFormat: the API-required format (e.g., year_int, iso8601_date)
 */
public class ValueConverter {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ValueConverter.class);

  private final TransformationRegistry transformationRegistry;

  public ValueConverter() {
    this.transformationRegistry = new TransformationRegistry();
  }

  public ValueConverter(TransformationRegistry transformationRegistry) {
    this.transformationRegistry = transformationRegistry;
  }

  /**
   * Convert a DTN value to the target format.
   * 
   * @param conceptualFieldKind conceptual field type (e.g., "date_yyyy_mm_dd")
   * @param targetFormat target format (e.g., "year_int", "iso8601_date")
   * @param value DTN string value
   * @return converted value (may be String, Number, Boolean, etc.)
   */
  public Object convert(String conceptualFieldKind, String targetFormat, String value) {
    if (value == null) {
      return null;
    }

    try {
      // Date conversions
      if (conceptualFieldKind != null && conceptualFieldKind.startsWith("date_")) {
        return convertDate(conceptualFieldKind, targetFormat, value);
      }

      // Number conversions
      if (conceptualFieldKind != null && conceptualFieldKind.contains("decimal") || 
          conceptualFieldKind != null && conceptualFieldKind.contains("amount")) {
        return convertNumber(conceptualFieldKind, targetFormat, value);
      }

      // Boolean conversions
      if ("true".equals(value) || "false".equals(value)) {
        return convertBoolean(targetFormat, value);
      }

      // Default: return as string
      return value;
    } catch (Exception e) {
      log.warn("Conversion failed for {} -> {} with value {}: {}", 
          conceptualFieldKind, targetFormat, value, e.getMessage());
      return value; // Fallback to original value
    }
  }

  private Object convertDate(String conceptualFieldKind, String targetFormat, String value) {
    try {
      Instant instant = parseDate(conceptualFieldKind, value);
      
      if (targetFormat == null) {
        return value;
      }

      switch (targetFormat) {
        case "year_int":
          return instant.atZone(ZoneId.systemDefault()).getYear();
        case "month_int":
          return instant.atZone(ZoneId.systemDefault()).getMonthValue();
        case "day_int":
          return instant.atZone(ZoneId.systemDefault()).getDayOfMonth();
        case "iso8601_date":
          return instant.atZone(ZoneId.systemDefault()).toLocalDate().toString();
        case "iso8601_datetime":
          return instant.toString();
        case "unix_ms":
          return instant.toEpochMilli();
        case "unix_s":
          return instant.getEpochSecond();
        default:
          // Try to parse as DateTimeFormatter pattern
          if (targetFormat.contains("y") || targetFormat.contains("M") || 
              targetFormat.contains("d") || targetFormat.contains("h") || 
              targetFormat.contains("m") || targetFormat.contains("s")) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(targetFormat);
            if (targetFormat.contains("h") || targetFormat.contains("m") || targetFormat.contains("s")) {
              LocalTime time = instant.atZone(ZoneId.systemDefault()).toLocalTime();
              return time.format(formatter);
            } else if (targetFormat.contains("y") || targetFormat.contains("M") || targetFormat.contains("d")) {
              LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
              return date.format(formatter);
            } else {
              LocalDateTime ldt = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
              return ldt.format(formatter);
            }
          }
          return value;
      }
    } catch (Exception e) {
      log.warn("Date conversion failed: {}", e.getMessage());
      return value;
    }
  }

  private Instant parseDate(String conceptualFieldKind, String value) {
    if (conceptualFieldKind == null) {
      // Try ISO 8601
      return Instant.parse(value);
    }

    switch (conceptualFieldKind) {
      case "date_yyyy":
        return LocalDate.parse(value + "-01-01", DateTimeFormatter.ISO_LOCAL_DATE)
            .atStartOfDay(ZoneId.systemDefault()).toInstant();
      case "date_yyyy_mm":
        return LocalDate.parse(value + "-01", DateTimeFormatter.ISO_LOCAL_DATE)
            .atStartOfDay(ZoneId.systemDefault()).toInstant();
      case "date_yyyy_mm_dd":
        return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE)
            .atStartOfDay(ZoneId.systemDefault()).toInstant();
      case "date_quarter":
        // Format: "2024-Q3"
        String[] parts = value.split("-Q");
        int year = Integer.parseInt(parts[0]);
        int quarter = Integer.parseInt(parts[1]);
        int month = (quarter - 1) * 3 + 1;
        return LocalDate.of(year, month, 1)
            .atStartOfDay(ZoneId.systemDefault()).toInstant();
      default:
        // Try ISO 8601
        try {
          return Instant.parse(value);
        } catch (Exception e) {
          throw new IllegalArgumentException("Unsupported date format: " + conceptualFieldKind);
        }
    }
  }

  private Object convertNumber(String conceptualFieldKind, String targetFormat, String value) {
    try {
      // Use transformation registry for type conversions
      if ("int".equals(targetFormat) || "integer".equals(targetFormat)) {
        return transformationRegistry.apply("str_to_int", value);
      }
      
      if ("long".equals(targetFormat)) {
        return transformationRegistry.apply("str_to_long", value);
      }
      
      if ("float".equals(targetFormat)) {
        return transformationRegistry.apply("str_to_float", value);
      }
      
      if ("double".equals(targetFormat)) {
        return transformationRegistry.apply("str_to_double", value);
      }
      
      if ("string".equals(targetFormat)) {
        return value;
      }
      
      // Default: try to parse as double
      double num = Double.parseDouble(value);
      if (targetFormat == null || "double".equals(targetFormat)) {
        return num;
      }
      
      return num;
    } catch (NumberFormatException e) {
      log.warn("Number conversion failed for value: {}", value);
      return value;
    }
  }

  private Object convertBoolean(String targetFormat, String value) {
    boolean bool = "true".equals(value);
    
    if (targetFormat == null || "boolean".equals(targetFormat)) {
      return bool;
    }
    
    if ("string".equals(targetFormat)) {
      return value;
    }
    
    if ("int".equals(targetFormat) || "integer".equals(targetFormat)) {
      return bool ? 1 : 0;
    }
    
    return bool;
  }
}

