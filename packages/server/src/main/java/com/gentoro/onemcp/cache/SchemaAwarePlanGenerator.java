package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.context.KnowledgeBase;
import com.gentoro.onemcp.context.Operation;
import com.gentoro.onemcp.context.Service;
import com.gentoro.onemcp.exception.ExecutionException;
import com.gentoro.onemcp.orchestrator.OrchestratorContext;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.prompt.PromptRepository;
import com.gentoro.onemcp.prompt.PromptTemplate;
import com.gentoro.onemcp.prompt.impl.PebblePromptTemplate;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generates parameterized execution plans from prompt + schema.
 *
 * <p>The generated plans use placeholders like {@code {{params.year}}} instead of
 * literal values, allowing cache reuse across different parameter values.
 */
public class SchemaAwarePlanGenerator {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(SchemaAwarePlanGenerator.class);

  private final OneMcp oneMcp;
  private final OrchestratorContext context;

  public SchemaAwarePlanGenerator(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
    this.context = null;
  }

  public SchemaAwarePlanGenerator(OrchestratorContext context) {
    this.oneMcp = context.oneMcp();
    this.context = context;
  }

  /**
   * Generate a parameterized execution plan from the prompt and schema.
   *
   * @param prompt the original natural-language prompt
   * @param schema the normalized PromptSchema
   * @return an ExecutionPlan with placeholders for parameter values
   * @throws ExecutionException if plan generation fails
   */
  public ExecutionPlan createPlan(String prompt, PromptSchema schema) throws ExecutionException {
    log.info("Generating parameterized plan for PSK: {}", schema.getCacheKey());
    log.debug("Original prompt: {}", prompt);
    log.debug("Schema: {}", schema);

    // Get available operations from knowledge base
    KnowledgeBase kb = oneMcp.knowledgeBase();
    List<String> availableOperations = getAvailableOperations(kb);
    log.debug("Available operations: {}", availableOperations);

    // Build operation documentation for the prompt
    String operationDocs = buildOperationDocumentation(kb);

    // Prepare prompt context
    Map<String, Object> promptContext = new HashMap<>();
    promptContext.put("original_prompt", prompt);
    promptContext.put("schema_json", serializeSchema(schema));
    promptContext.put("available_operations", availableOperations);
    promptContext.put("operation_docs", operationDocs);
    promptContext.put("param_keys", new ArrayList<>(schema.getParams().keySet()));

    // Load and render the plan generation prompt
    PromptTemplate template = null;
    
    // Try to load from repository first
    try {
      PromptRepository promptRepo = null;
      if (this.context != null) {
        promptRepo = this.context.prompts();
      }
      if (promptRepo == null) {
        promptRepo = oneMcp.promptRepository();
      }
      
      if (promptRepo != null) {
        try {
          template = promptRepo.get("plan-generation-schema-aware");
        } catch (Exception e) {
          // Try with leading slash
          template = promptRepo.get("/plan-generation-schema-aware");
        }
      }
    } catch (Exception e) {
      log.warn("Failed to load prompt from repository, falling back to embedded template: {}", e.getMessage());
    }
    
    // Use fallback if repository failed
    if (template == null) {
      log.info("Using embedded fallback prompt template");
      template = createFallbackTemplate();
    }
    
    PromptTemplate.PromptSession session = template.newSession();
    session.enable("plan-generation-schema-aware", promptContext);

    List<LlmClient.Message> messages = session.renderMessages();
    if (messages.isEmpty()) {
      throw new ExecutionException("No messages rendered from plan generation prompt");
    }

    // Call LLM with retry
    LlmClient llmClient = oneMcp.llmClient();
    if (llmClient == null) {
      throw new ExecutionException("LLM client not available");
    }

    // Log phase change for plan generation
    if (oneMcp.inferenceLogger() != null) {
      oneMcp.inferenceLogger().logPhaseChange("plan");
    }

    int maxAttempts = 3;
    List<String> previousErrors = new ArrayList<>();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      List<LlmClient.Message> messagesToSend = new ArrayList<>(messages);

      // Add feedback from previous attempt if retrying
      if (attempt > 1 && !previousErrors.isEmpty()) {
        StringBuilder feedback = new StringBuilder();
        feedback.append("Your previous response was REJECTED due to errors:\n\n");
        for (String error : previousErrors) {
          feedback.append("❌ ").append(error).append("\n");
        }
        feedback.append("\nPlease fix these issues and try again.\n");
        messagesToSend.add(new LlmClient.Message(LlmClient.Role.USER, feedback.toString()));
        log.warn("Retrying plan generation (attempt {}/{})", attempt, maxAttempts);
      }

      try {
        // Create telemetry sink to set phase for proper logging
        final Map<String, Object> sinkAttributes = new HashMap<>();
        sinkAttributes.put("phase", "plan"); // Set phase so LLM client detects it correctly
        
        LlmClient.TelemetrySink tokenSink = new LlmClient.TelemetrySink() {
          @Override
          public void startChild(String name) {}
          
          @Override
          public void endCurrentOk(java.util.Map<String, Object> attrs) {}
          
          @Override
          public void endCurrentError(java.util.Map<String, Object> attrs) {}
          
          @Override
          public void addUsage(Long promptTokens, Long completionTokens, Long totalTokens) {}
          
          @Override
          public java.util.Map<String, Object> currentAttributes() {
            return sinkAttributes;
          }
        };
        
        String response;
        try (LlmClient.TelemetryScope ignored = llmClient.withTelemetry(tokenSink)) {
          response = llmClient.chat(messagesToSend, Collections.emptyList(), false, null);
        }
        
        // Extract JSON from response
        String jsonStr = extractJsonFromResponse(response);
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
          previousErrors.clear();
          previousErrors.add("No JSON content found in response");
          continue;
        }

        // Parse and validate the plan
        JsonNode planNode = JacksonUtility.getJsonMapper().readTree(jsonStr);
        
        // Basic validation
        List<String> validationErrors = validatePlan(planNode, availableOperations);
        if (!validationErrors.isEmpty()) {
          previousErrors.clear();
          previousErrors.addAll(validationErrors);
          continue;
        }

        // Success!
        ExecutionPlan plan = ExecutionPlan.fromNode(planNode);
        log.info("Successfully generated plan (attempt {})", attempt);
        log.debug("Generated plan: {}", plan.toPrettyJson());
        return plan;

      } catch (Exception e) {
        log.error("Plan generation failed (attempt {}): {}", attempt, e.getMessage());
        previousErrors.clear();
        previousErrors.add("Exception: " + e.getMessage());
      }
    }

    throw new ExecutionException(
        "Failed to generate execution plan after " + maxAttempts + " attempts. " +
        "Last errors: " + String.join(", ", previousErrors));
  }

  /**
   * Get list of available operation names from the knowledge base.
   */
  private List<String> getAvailableOperations(KnowledgeBase kb) {
    if (kb == null || kb.services() == null) {
      return Collections.emptyList();
    }
    return kb.services().stream()
        .filter(s -> s.getOperations() != null)
        .flatMap(s -> s.getOperations().stream())
        .map(Operation::getOperation)
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Build operation documentation string for the prompt.
   */
  private String buildOperationDocumentation(KnowledgeBase kb) {
    if (kb == null || kb.services() == null) {
      return "No operations available.";
    }

    StringBuilder sb = new StringBuilder();
    for (Service service : kb.services()) {
      if (service.getOperations() == null || service.getOperations().isEmpty()) {
        continue;
      }
      
      sb.append("### Service: ").append(service.getSlug()).append("\n\n");
      
      for (Operation op : service.getOperations()) {
        sb.append("#### `").append(op.getOperation()).append("`\n");
        if (op.getSummary() != null && !op.getSummary().isEmpty()) {
          sb.append(op.getSummary()).append("\n");
        }
        sb.append("\n");
      }
    }
    
    return sb.length() > 0 ? sb.toString() : "No operations available.";
  }

  /**
   * Serialize PromptSchema to JSON string for inclusion in the prompt.
   */
  private String serializeSchema(PromptSchema schema) {
    try {
      return JacksonUtility.getJsonMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(schema);
    } catch (Exception e) {
      log.error("Failed to serialize schema", e);
      return "{}";
    }
  }

  /**
   * Extract JSON content from LLM response.
   */
  private String extractJsonFromResponse(String response) {
    if (response == null || response.trim().isEmpty()) {
      return null;
    }

    String jsonStr = response.trim();

    // Remove markdown code block markers
    if (jsonStr.startsWith("```json")) {
      jsonStr = jsonStr.substring(7).trim();
    } else if (jsonStr.startsWith("```")) {
      jsonStr = jsonStr.substring(3).trim();
    }

    if (jsonStr.endsWith("```")) {
      jsonStr = jsonStr.substring(0, jsonStr.length() - 3).trim();
    }

    // Find JSON object boundaries
    int firstBrace = jsonStr.indexOf('{');
    int lastBrace = jsonStr.lastIndexOf('}');

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      jsonStr = jsonStr.substring(firstBrace, lastBrace + 1);
    }

    return jsonStr.trim();
  }

  /**
   * Validate the generated plan.
   */
  private List<String> validatePlan(JsonNode plan, List<String> availableOperations) {
    List<String> errors = new ArrayList<>();

    // Must have start_node
    if (!plan.has("start_node")) {
      errors.add("Plan must have a 'start_node'");
    }

    // Check all operation nodes use valid operations
    plan.fields().forEachRemaining(entry -> {
      String nodeId = entry.getKey();
      JsonNode nodeDef = entry.getValue();
      
      if (nodeDef.has("operation")) {
        String opName = nodeDef.get("operation").asText();
        if (!availableOperations.contains(opName)) {
          errors.add("Node '" + nodeId + "' uses unknown operation: " + opName + 
              ". Available: " + availableOperations);
        }
      }
    });

    // Must have at least one terminal node (completed: true)
    boolean hasTerminal = false;
    var fields = plan.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      JsonNode nodeDef = entry.getValue();
      if (nodeDef.has("completed") && nodeDef.get("completed").asBoolean()) {
        hasTerminal = true;
        break;
      }
    }
    if (!hasTerminal) {
      errors.add("Plan must have at least one terminal node with 'completed: true'");
    }

    return errors;
  }

  private PromptTemplate createFallbackTemplate() {
    String systemContent = """
You are an Execution Plan Generator.

Your job is to generate a **parameterized JSON execution plan** that can be cached
and reused with different parameter values. The plan uses placeholders for values
that will be filled in at execution time.

======================================================================
## 1. PLACEHOLDER SYNTAX
======================================================================

Use placeholders for parameter values from the schema:

- Format: `{{ "{{" }}params.<field_name>{{ "}}" }}`
- Nested: `{{ "{{" }}params.<field>.<subfield>{{ "}}" }}`

Examples:
- `{{ "{{" }}params.year{{ "}}" }}` → will be replaced with the actual year value
- `{{ "{{" }}params.amount.aggregate{{ "}}" }}` → will be replaced with "sum", "avg", etc.

**CRITICAL**: Use placeholders for ALL parameter values. Do NOT hardcode values
like "2024" or "sum" - use `{{ "{{" }}params.year{{ "}}" }}` and `{{ "{{" }}params.amount.aggregate{{ "}}" }}`.

======================================================================
## 2. PLAN STRUCTURE
======================================================================

The execution plan is a JSON object with node definitions:

```json
{
  "start_node": {
    "vars": {
      "variable_name": "{{ "{{" }}params.<field>{{ "}}" }}"
    },
    "route": "<next_node_id>"
  },
  
  "<operation_node_id>": {
    "operation": "<operation_name>",
    "input": {
      // Operation-specific input, using placeholders
    },
    "route": "<next_node_id>"  // or conditional routing
  },
  
  "<terminal_node_id>": {
    "completed": true,
    "vars": {
      "answer": "$.['<operation_node_id>'].['data']"
    }
  }
}
```

======================================================================
## 3. NODE TYPES
======================================================================

### start_node (REQUIRED)
Entry point. Define input variables and route to first operation.

```json
"start_node": {
  "vars": {
    "year_filter": "{{ "{{" }}params.year{{ "}}" }}"
  },
  "route": "query_data"
}
```

### Operation Node
Calls a registered operation with input parameters.

```json
"query_data": {
  "operation": "querySalesData",
  "input": {
    "filter": [
      { "field": "date.year", "operator": "equals", "value": "{{ "{{" }}params.year{{ "}}" }}" }
    ],
    "aggregates": [
      { "field": "sale.amount", "function": "{{ "{{" }}params.amount.aggregate{{ "}}" }}", "alias": "total" }
    ]
  },
  "route": "summary"
}
```

### Terminal Node (REQUIRED)
Marks completion and extracts final output.

```json
"summary": {
  "completed": true,
  "vars": {
    "answer": "$.['query_data'].['data']"
  }
}
```

======================================================================
## 4. ROUTING
======================================================================

Routes can be:
- Simple string: `"route": "next_node"`
- Conditional array:
  ```json
  "route": [
    { "condition": "$.['node'].['success'] == true", "node": "success_node" },
    "fallback_node"
  ]
  ```

======================================================================
## 5. JSONPATH EXPRESSIONS
======================================================================

Use JSONPath to reference previous node outputs:
- `$.['node_id'].['field']` - access a field from a node's output
- Always use bracket notation: `$.['node'].['field']`

======================================================================
## 6. AVAILABLE OPERATIONS
======================================================================

You may ONLY use these operations (from the knowledge base):

{{ operation_docs | raw }}

Operation names to use: {{ available_operations }}

**CRITICAL**: Do NOT invent operations. Only use operations from the list above.

======================================================================
## 7. EXAMPLE
======================================================================

For a schema like:
```json
{
  "action": "summarize",
  "entities": ["sale"],
  "group_by": ["category"],
  "params": {
    "year": "2024",
    "amount": { "aggregate": "sum" }
  }
}
```

Generate a plan like:
```json
{
  "start_node": {
    "vars": {
      "year": "{{ "{{" }}params.year{{ "}}" }}"
    },
    "route": "query_sales"
  },
  "query_sales": {
    "operation": "querySalesData",
    "input": {
      "filter": [
        { "field": "date.year", "operator": "equals", "value": "{{ "{{" }}params.year{{ "}}" }}" }
      ],
      "aggregates": [
        { "field": "sale.amount", "function": "{{ "{{" }}params.amount.aggregate{{ "}}" }}", "alias": "total" }
      ],
      "fields": ["product.category"]
    },
    "route": [
      { "condition": "$.['query_sales'].['success'] == true", "node": "summary" },
      "error"
    ]
  },
  "error": {
    "completed": true,
    "vars": {
      "answer": "Failed to retrieve sales data."
    }
  },
  "summary": {
    "completed": true,
    "vars": {
      "answer": "$.['query_sales'].['data']"
    }
  }
}
```

======================================================================
## 8. OUTPUT REQUIREMENTS
======================================================================

- Output ONLY valid JSON (no markdown, no explanations)
- Must have `start_node`
- Must have at least one terminal node with `completed: true`
- All operation names must be from the available operations list
- Use placeholders for ALL parameter values
""";

    String userContent = """
Generate a parameterized execution plan for the following:

**Original Prompt:**
{{ original_prompt | raw }}

**Normalized Schema:**
{{ schema_json | raw }}

**Parameter Keys to use as placeholders:**
{{ param_keys }}

Generate the JSON execution plan using `{{ "{{" }}params.<key>{{ "}}" }}` placeholders for each
parameter value. Output ONLY the JSON, no explanations.
""";

    List<PromptTemplate.PromptSection> sections = new ArrayList<>();
    sections.add(new PromptTemplate.PromptSection(
        LlmClient.Role.SYSTEM, 
        "plan-generation-schema-aware", 
        true, 
        systemContent));
    
    sections.add(new PromptTemplate.PromptSection(
        LlmClient.Role.USER, 
        "plan-generation-schema-aware", 
        true, 
        userContent));

    return new PebblePromptTemplate("plan-generation-schema-aware", sections);
  }
}

