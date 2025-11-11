package com.gentoro.onemcp.utility;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.gentoro.onemcp.exception.SerializationException;

public class JacksonUtility {
  private static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(
          new YAMLFactory()
              .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
              .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE)
              .disable(YAMLGenerator.Feature.SPLIT_LINES)
              .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));

  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper()
          // 👇 Ignore extra fields in JSON that aren't in your class
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

          // 👇 Allow serialization even if beans have no properties
          .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

          // 👇 Indent output for readability (pretty-print)
          .enable(SerializationFeature.INDENT_OUTPUT)

          // 👇 Include only non-null fields in output
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  public static ObjectMapper getYamlMapper() {
    return YAML_MAPPER;
  }

  public static ObjectMapper getJsonMapper() {
    return JSON_MAPPER;
  }

  public static String toJson(Object object) {
    try {
      return JSON_MAPPER.writeValueAsString(object);
    } catch (Exception e) {
      throw new SerializationException("Failed to serialize object to JSON", e);
    }
  }
}
