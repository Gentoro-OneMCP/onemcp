package com.gentoro.onemcp.indexing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ContextDecorator {
  private final List<Map<String, Object>> retrievedContextualData;

  public ContextDecorator(List<Map<String, Object>> retrievedContextualData) {
    this.retrievedContextualData = retrievedContextualData;
  }

  public String getGeneralDocs() {
    return retrievedContextualData.stream()
        .filter(m -> m.get("nodeType").equals("DOCS_CHUNK"))
        .map(m -> (String) m.get("content"))
        .collect(Collectors.joining("\n\n"));
  }

  public List<Api> getApiDocs() {
    Set<String> apiSlugs =
        retrievedContextualData.stream()
            .filter(m -> m.get("nodeType").equals("API_DOCUMENTATION"))
            .map(m -> (String) m.get("apiSlug"))
            .collect(Collectors.toSet());

    return apiSlugs.stream()
        .map(
            slug -> {
              Set<String> operationIds =
                  retrievedContextualData.stream()
                      .filter(
                          m ->
                              m.get("nodeType").equals("API_OPERATION_DOCUMENTATION")
                                  && m.get("apiSlug").equals(slug))
                      .map(m -> (String) m.get("operationId"))
                      .collect(Collectors.toSet());

              String apiDocumentation =
                  retrievedContextualData.stream()
                      .filter(
                          m ->
                              m.get("nodeType").equals("API_DOCUMENTATION")
                                  && m.get("apiSlug").equals(slug))
                      .map(m -> (String) m.get("content"))
                      .collect(Collectors.joining("\n\n"));

              List<Operation> operations =
                  operationIds.stream()
                      .map(
                          operationId -> {
                            String documentation =
                                retrievedContextualData.stream()
                                    .filter(
                                        m ->
                                            m.get("nodeType").equals("API_OPERATION_DOCUMENTATION")
                                                && m.get("apiSlug").equals(slug)
                                                && m.get("operationId").equals(operationId))
                                    .map(m -> (String) m.get("content"))
                                    .collect(Collectors.joining("\n\n"));

                            String inputSchema =
                                retrievedContextualData.stream()
                                    .filter(
                                        m ->
                                            m.get("nodeType").equals("API_OPERATION_INPUT")
                                                && m.get("apiSlug").equals(slug)
                                                && m.get("operationId").equals(operationId))
                                    .map(m -> (String) m.get("content"))
                                    .collect(Collectors.joining("\n\n"));

                            String outputSchema =
                                retrievedContextualData.stream()
                                    .filter(
                                        m ->
                                            m.get("nodeType").equals("API_OPERATION_OUTPUT")
                                                && m.get("apiSlug").equals(slug)
                                                && m.get("operationId").equals(operationId))
                                    .map(m -> (String) m.get("content"))
                                    .collect(Collectors.joining("\n\n"));

                            List<Example> examples =
                                retrievedContextualData.stream()
                                    .filter(
                                        m ->
                                            m.get("nodeType").equals("API_OPERATION_EXAMPLE")
                                                && m.get("apiSlug").equals(slug)
                                                && m.get("operationId").equals(operationId))
                                    .map(
                                        m ->
                                            new Example(
                                                (String) m.get("title"),
                                                (String) m.get("summary"),
                                                (String) m.get("content")))
                                    .toList();

                            return new Operation(
                                operationId, documentation, inputSchema, outputSchema, examples);
                          })
                      .toList();

              return new Api(slug, apiDocumentation, operations);
            })
        .toList();
  }

  public static class Api {
    String slug;
    String documentation;
    List<Operation> operations;

    public Api(String slug, String documentation, List<Operation> operations) {
      this.slug = slug;
      this.documentation = documentation;
      this.operations = operations;
    }

    public String getSlug() {
      return slug;
    }

    public String getDocumentation() {
      return documentation;
    }

    public List<Operation> getOperations() {
      return operations;
    }
  }

  public static class Operation {

    String operationId;
    String documentation;
    String inputSchema;
    String outputSchema;
    List<Example> examples;

    public Operation(
        String operationId,
        String documentation,
        String inputSchema,
        String outputSchema,
        List<Example> examples) {
      this.operationId = operationId;
      this.documentation = documentation;
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
      this.examples = examples;
    }

    public String getOperationId() {
      return operationId;
    }

    public String getDocumentation() {
      return documentation;
    }

    public String getInputSchema() {
      return inputSchema;
    }

    public String getOutputSchema() {
      return outputSchema;
    }

    public List<Example> getExamples() {
      return examples;
    }
  }

  public static class Example {
    String title;
    String summary;
    String content;

    public Example(String title, String summary, String content) {
      this.title = title;
      this.summary = summary;
      this.content = content;
    }

    public String getTitle() {
      return title;
    }

    public String getSummary() {
      return summary;
    }

    public String getContent() {
      return content;
    }
  }
}
