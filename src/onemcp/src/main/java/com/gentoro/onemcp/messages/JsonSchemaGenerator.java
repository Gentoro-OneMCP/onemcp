package com.gentoro.onemcp.messages;

import com.fasterxml.jackson.databind.node.*;
import com.gentoro.onemcp.exception.ValidationException;
import com.gentoro.onemcp.model.Tool;
import com.gentoro.onemcp.model.ToolDefinition;
import com.gentoro.onemcp.model.ToolProperty;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;

public class JsonSchemaGenerator {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(JsonSchemaGenerator.class);

  public static ToolProperty generateSchema(Class<?> clazz, String description) {
    return ToolProperty.builder()
        .name(clazz.getSimpleName())
        .type(ToolProperty.Type.OBJECT)
        .description(description)
        .properties(generateProperties(clazz))
        .build();
  }

  private static List<ToolProperty> generateProperties(Class<?> clazz) {
    List<ToolProperty> props = new ArrayList<>();

    for (RecordComponent rc : clazz.getRecordComponents()) {
      ToolProperty.Builder builder = ToolProperty.builder();

      String name = rc.getName();
      Class<?> type = rc.getType();

      FieldDoc fieldDoc = rc.getAnnotation(FieldDoc.class);

      builder.name(name);
      builder.required(fieldDoc.required());

      if (fieldDoc != null && !fieldDoc.description().isEmpty())
        builder.description(fieldDoc.description());

      ToolProperty.Type inferredType = inferJsonType(type);
      builder.type(inferredType);

      if (inferredType == ToolProperty.Type.OBJECT) {
        builder.properties(generateProperties(type));
      } else if (inferredType == ToolProperty.Type.ARRAY) {
        builder.items(handleCollectionType(rc));
      }
      props.add(builder.build());
    }
    return props;
  }

  private static ToolProperty.Type inferJsonType(Class<?> type) {
    if (String.class.equals(type)) return ToolProperty.Type.STRING;
    if (Number.class.isAssignableFrom(type) || type.isPrimitive()) return ToolProperty.Type.NUMBER;
    if (Boolean.class.equals(type) || boolean.class.equals(type)) return ToolProperty.Type.BOOLEAN;
    if (Collection.class.isAssignableFrom(type)) return ToolProperty.Type.ARRAY;
    if (type.isRecord() || !type.isPrimitive()) return ToolProperty.Type.OBJECT;
    return ToolProperty.Type.STRING;
  }

  private static ToolProperty handleCollectionType(RecordComponent rc) {
    ToolProperty.Builder builder = ToolProperty.builder();
    Type type = rc.getGenericType();
    if (type instanceof ParameterizedType parameterized) {
      Type[] args = parameterized.getActualTypeArguments();
      if (args.length == 1 && args[0] instanceof Class<?> c) {
        builder.type(inferJsonType(c));
        builder.description(rc.getAnnotation(FieldDoc.class).description());
        if (c.isRecord()) {
          builder.properties(generateProperties(c));
        }
      }
    }
    return builder.build();
  }

  public static <T> Tool asCapturingTool(Class<T> clazz, Function<T, String> caller) {
    return asCapturingTool(
        clazz, "Call this Tool / Function to share the result of the reasoning.", caller);
  }

  public static <T> Tool asCapturingTool(
      Class<T> clazz, String description, Function<T, String> caller) {
    return new Tool() {
      @Override
      public String name() {
        return clazz.getSimpleName();
      }

      @Override
      public String summary() {
        return description;
      }

      @Override
      public ToolDefinition definition() {
        return new ToolDefinition(name(), summary(), generateSchema(clazz, summary()));
      }

      @Override
      public String execute(Map<String, Object> args) {
        log.trace("Executing tool: {} with args: {}", name(), JacksonUtility.toJson(args));

        if (clazz.equals(ExecutionPlan.class)) {
          List<Map<String, Object>> step =
              args.get("steps") != null
                  ? (List<Map<String, Object>>) args.get("steps")
                  : new ArrayList<>();
          List<ExecutionPlan.Step> listOfSteps = new ArrayList<>();
          for (Map<String, Object> s : step) {
            String description = (String) s.get("description");
            String title = (String) s.get("title");
            List<Map<String, Object>> services =
                s.get("services") != null
                    ? (List<Map<String, Object>>) s.get("services")
                    : new ArrayList<>();
            List<ExecutionPlan.Step.Service> listOfServices = new ArrayList<>();
            for (Map<String, Object> srv : services) {
              listOfServices.add(
                  new ExecutionPlan.Step.Service(
                      srv.get("serviceName").toString(), (List<String>) srv.get("operations")));
            }
            listOfSteps.add(new ExecutionPlan.Step(title, listOfServices, description));
          }
          return caller.apply(clazz.cast(new ExecutionPlan(listOfSteps)));
        } else if (clazz.equals(StepImplementation.class)) {

          if (args.get("qualifiedClassName") == null
              || ((String) args.get("qualifiedClassName")).isEmpty()) {
            log.warn("Missing `qualifiedClassName` in StepImplementation call.");
            return "Error: Call to Function / Tool named `ExecutionPlan` was wrongly formatted. The `qualifiedClassName` is required and was not provided. Review the provided instructions and retry.";
          }

          if (args.get("snippet") == null || ((String) args.get("snippet")).isEmpty()) {
            log.warn("Missing `snippet` in StepImplementation call.");
            return "Error: Call to Function / Tool named `ExecutionPlan` was wrongly formatted. The `snippet` is required and was not provided. Review the provided instructions and retry.";
          }

          if (args.get("explanation") == null || ((String) args.get("explanation")).isEmpty()) {
            log.warn("Missing `explanation` in StepImplementation call.");
            return "Error: Call to Function / Tool named `ExecutionPlan` was wrongly formatted. The `explanation` is required and was not provided. Review the provided instructions and retry.";
          }

          return caller.apply(
              clazz.cast(
                  new StepImplementation(
                      (String) args.get("qualifiedClassName"),
                      (String) args.get("snippet"),
                      (String) args.get("explanation"))));
        } else if (clazz.equals(Summary.class)) {
          if (args.get("answer") == null || ((String) args.get("answer")).isEmpty()) {
            log.warn("Missing `answer` in StepImplementation call.");
            return "Error: Call to Function / Tool named `StepImplementation` was wrongly formatted. The `answer` is required and was not provided. Review the provided instructions and retry.";
          }

          return caller.apply(
              clazz.cast(new Summary((String) args.get("answer"), (String) args.get("reasoning"))));
        } else if (clazz.equals(Documentation.class)) {
          if (args.get("services") == null
              || !(args.get("services") instanceof List)
              || ((List) args.get("services")).isEmpty()) {
            log.warn("Missing `services` in StepImplementation call.");
            return "Error: Call to Function / Tool named `Documentation` was wrongly formatted. The `services` is required and was not provided. Review the provided instructions and retry.";
          }
          return caller.apply(clazz.cast(new Documentation((List<String>) args.get("services"))));
        } else {
          throw new ValidationException(
              "Capturing tool was reported with a tool name that is currently not mapped: "
                  + clazz.getName()
                  + ".\n"
                  + "This could mean a newly introduced message type that is not yet mapped, or a wrong tool call issued by an LLM.");
        }
      }
    };
  }
}
