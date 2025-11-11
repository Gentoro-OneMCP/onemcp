package com.gentoro.onemcp.memory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;

public class Value {
  private String identifier;
  private Object content;
  private String description;
  private static SchemaGenerator generator = null;

  static {
    SchemaGeneratorConfigBuilder configBuilder =
        new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON);
    generator = new SchemaGenerator(configBuilder.build());
  }

  public Value() {}

  public Value(String identifier, Object content, String description) {
    this.identifier = identifier;
    this.content = content;
    this.description = description;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public Object getContent() {
    return content;
  }

  public void setContent(Object content) {
    this.content = content;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    try {
      if (content != null) {
        String jsonValue = JacksonUtility.getJsonMapper().writeValueAsString(content);
        if (jsonValue.length() > 800) {
          return """
              %s

              > This entry contains a large amount of data (%d bytes). Defer from pulling all content if possible, instead create a Java Snippet that can aggregate the data accordingly to your needs.
              """
              .formatted(description, jsonValue.length());
        }
      }
    } catch (JsonProcessingException ignored) {
    }
    return description;
  }

  public String getModel() {
    if (getContent() == null) {
      return "Current value is null, there are no information about the data model for this entry.";
    } else {
      return generator.generateSchema(content.getClass()).toPrettyString();
    }
  }
}
