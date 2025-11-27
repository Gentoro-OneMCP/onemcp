# Prompt Schema → Execution Plan Caching

## Sprint Goal

Enable caching of execution plans keyed by Prompt Schema Keys (PSK), allowing similar prompts
to reuse pre-generated plans with different parameter values, significantly reducing LLM inference 
costs and latency.

---

## 1. Major Operations

The orchestrator uses **five major operations** to handle prompts. Each operation works with 
first-class domain objects.

### 1.1 Operations Summary

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           MAJOR OPERATIONS                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  1. NORMALIZE PROMPT                                                             │
│     PromptSchema normalizePrompt(String prompt)                                 │
│                                                                                  │
│  2. LOOKUP PLAN                                                                  │
│     Optional<ExecutionPlan> lookupPlan(PromptSchema schema)                     │
│                                                                                  │
│  3. CREATE PLAN                                                                  │
│     ExecutionPlan createPlan(String prompt, PromptSchema schema)                │
│                                                                                  │
│  4. EXECUTE PLAN                                                                 │
│     String plan.execute(PromptSchema schema, OperationRegistry registry)        │
│                                                                                  │
│  5. STORE PLAN                                                                   │
│     void storePlan(PromptSchema schema, ExecutionPlan plan)                     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Operation: normalizePrompt

**Signature**:
```java
PromptSchema normalizePrompt(String prompt)
```

**Purpose**: Convert a natural-language prompt into a canonical PromptSchema.

**Input**:
- `prompt`: User's natural-language prompt (e.g., "Show me total sales in 2024 by category")

**Output**:
- `PromptSchema`: Structured representation with:
  - `action`: Canonical verb (e.g., "summarize")
  - `entities`: Resource types (e.g., ["sale"])
  - `groupBy`: Grouping fields (e.g., ["category"])
  - `params`: Field → value map (e.g., {"year": "2024", "amount": {"aggregate": "sum"}})
  - `cacheKey`: Deterministic PSK derived from structure (excludes param values)

**Example**:
```
Input:  "Show me total sales in 2024 by category"

Output: PromptSchema {
          action: "summarize",
          entities: ["sale"],
          groupBy: ["category"],
          params: { "year": "2024", "amount": { "aggregate": "sum" } },
          cacheKey: "summarize-sale-amount_year-group_category"
        }
```

---

### 1.3 Operation: lookupPlan

**Signature**:
```java
Optional<ExecutionPlan> lookupPlan(PromptSchema schema)
```

**Purpose**: Check cache for an existing execution plan.

**Input**:
- `schema`: The normalized PromptSchema (uses its `cacheKey`)

**Output**:
- `Optional<ExecutionPlan>`: The cached plan if found, empty otherwise

**Notes**:
- Cache treats `ExecutionPlan` as opaque - doesn't inspect its internals
- Uses `schema.cacheKey` to locate the file

---

### 1.4 Operation: createPlan

**Signature**:
```java
ExecutionPlan createPlan(String prompt, PromptSchema schema)
```

**Purpose**: Generate a new execution plan from the prompt and schema.

**Input**:
- `prompt`: Original natural-language prompt
- `schema`: Normalized schema (provides structure information)
- (Internal) Access to KnowledgeBase for available operations

**Output**:
- `ExecutionPlan`: A parameterized plan with placeholders like `{{params.year}}`

**Notes**:
- Uses LLM to generate the plan
- Placeholders allow cache reuse across different parameter values

---

### 1.5 Operation: executePlan (Object-Oriented)

**Signature**:
```java
String plan.execute(PromptSchema schema, OperationRegistry registry)
```

**Purpose**: Execute the plan with parameter values from the schema.

**Input**:
- `schema`: Contains actual parameter values in `params`
- `registry`: Available operations for the plan to invoke

**Output**:
- `String`: Text response/answer

**Notes**:
- `ExecutionPlan` handles hydration internally (replaces `{{params.X}}` with values)
- Caller doesn't need to know about placeholders or the execution engine

---

### 1.6 Operation: storePlan

**Signature**:
```java
void storePlan(PromptSchema schema, ExecutionPlan plan)
```

**Purpose**: Cache an execution plan for future reuse.

**Input**:
- `schema`: Uses its `cacheKey` as the storage key
- `plan`: The `ExecutionPlan` to cache

**Notes**:
- Cache asks `plan.toJson()` to serialize - doesn't inspect internals
- Stores in `<handbook>/plans/<cacheKey>.json`

---

## 2. Orchestrator Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATOR MAIN LOOP                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  handlePrompt(prompt):                                                           │
│                                                                                  │
│    ┌──────────────────────────────────────────────────────────────────┐         │
│    │  1. PromptSchema schema = normalizePrompt(prompt)                │         │
│    └──────────────────────────────────────────────────────────────────┘         │
│                              │                                                   │
│                              ▼                                                   │
│    ┌──────────────────────────────────────────────────────────────────┐         │
│    │  2. Optional<ExecutionPlan> cached = lookupPlan(schema)          │         │
│    └──────────────────────────────────────────────────────────────────┘         │
│                              │                                                   │
│              ┌───────────────┴───────────────┐                                  │
│              ▼                               ▼                                  │
│        cached.isPresent()             cached.isEmpty()                          │
│              │                               │                                  │
│              │                               ▼                                  │
│              │         ┌──────────────────────────────────────────────┐         │
│              │         │  3. ExecutionPlan plan = createPlan(         │         │
│              │         │          prompt, schema)                     │         │
│              │         └──────────────────────────────────────────────┘         │
│              │                               │                                  │
│              │                               ▼                                  │
│              │         ┌──────────────────────────────────────────────┐         │
│              │         │  5. storePlan(schema, plan)                  │         │
│              │         └──────────────────────────────────────────────┘         │
│              │                               │                                  │
│              └───────────────┬───────────────┘                                  │
│                              ▼                                                   │
│    ┌──────────────────────────────────────────────────────────────────┐         │
│    │  4. return plan.execute(schema, registry)                        │         │
│    └──────────────────────────────────────────────────────────────────┘         │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Pseudocode**:
```java
public String handlePrompt(String prompt) {
    // 1. Normalize
    PromptSchema schema = normalizePrompt(prompt);
    
    // 2. Lookup
    Optional<ExecutionPlan> cached = cache.lookup(schema);
    
    ExecutionPlan plan;
    if (cached.isPresent()) {
        plan = cached.get();
        log.info("Cache HIT for PSK: {}", schema.getCacheKey());
    } else {
        // 3. Create
        plan = generator.createPlan(prompt, schema);
        // 5. Store
        cache.store(schema, plan);
        log.info("Cache MISS - created and cached PSK: {}", schema.getCacheKey());
    }
    
    // 4. Execute (object-oriented style)
    return plan.execute(schema, registry);
}
```

---

## 3. First-Class Domain Objects

Every major concept in the spec has a corresponding Java class.

### 3.1 PromptSchema (Existing - Minor Updates)

```java
/**
 * Canonical representation of a normalized prompt.
 * The cacheKey is derived from structure only (excludes param values).
 */
public class PromptSchema {
    private String action;              // Canonical verb: "summarize", "search"
    private List<String> entities;      // Resource types: ["sale"]
    private List<String> groupBy;       // Grouping fields: ["category"]
    private Map<String, Object> params; // Field → value (includes actual VALUES)
    private String cacheKey;            // PSK: from structure only (excludes values)
    
    public void generateCacheKey() {
        // Key = (action, sorted(entities), sorted(params.keys()), groupBy)
        // Note: param VALUES not included!
    }
}
```

**Why cacheKey excludes values**: Two prompts with same structure but different values should 
share the same cached plan:
```
"sales in 2024 by category" → PSK: summarize-sale-amount_year-group_category
"sales in 2023 by category" → PSK: summarize-sale-amount_year-group_category  (SAME!)
```

---

### 3.2 ExecutionPlan (New)

```java
/**
 * First-class representation of an execution plan.
 * Encapsulates the JSON structure and knows how to execute itself.
 */
public class ExecutionPlan {
    private final JsonNode planNode;  // Internal - not exposed
    
    // ─────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────
    
    private ExecutionPlan(JsonNode planNode) {
        this.planNode = planNode;
    }
    
    /** Parse from JSON string (e.g., from cache file) */
    public static ExecutionPlan fromJson(String json) {
        JsonNode node = JacksonUtility.getJsonMapper().readTree(json);
        return new ExecutionPlan(node);
    }
    
    /** Create from JsonNode (e.g., from LLM response) */
    public static ExecutionPlan fromNode(JsonNode node) {
        return new ExecutionPlan(node);
    }
    
    // ─────────────────────────────────────────────────────────────────
    // Serialization
    // ─────────────────────────────────────────────────────────────────
    
    /** Serialize to JSON string (for cache storage) */
    public String toJson() {
        return JacksonUtility.getJsonMapper().writeValueAsString(planNode);
    }
    
    // ─────────────────────────────────────────────────────────────────
    // Execution
    // ─────────────────────────────────────────────────────────────────
    
    /**
     * Execute this plan with parameter values from the schema.
     * Handles hydration internally - caller doesn't see placeholders.
     */
    public String execute(PromptSchema schema, OperationRegistry registry) {
        // 1. Hydrate placeholders: {{params.year}} → "2024"
        JsonNode hydratedPlan = hydrate(this.planNode, schema.getParams());
        
        // 2. Execute via engine (internal detail)
        ExecutionPlanEngine engine = new ExecutionPlanEngine(
            JacksonUtility.getJsonMapper(), registry);
        JsonNode result = engine.execute(hydratedPlan, null);
        
        // 3. Format result
        return formatResult(result);
    }
    
    // ─────────────────────────────────────────────────────────────────
    // Internal
    // ─────────────────────────────────────────────────────────────────
    
    private JsonNode hydrate(JsonNode plan, Map<String, Object> params) {
        // Recursively replace {{params.X}} with values from params map
        // Handles nested: {{params.amount.aggregate}} → params.get("amount").get("aggregate")
    }
    
    private String formatResult(JsonNode result) {
        // Convert execution result to user-friendly text
    }
}
```

**Key Design Points**:
- `JsonNode` is internal - not exposed in any public API
- `execute()` handles hydration - caller just passes schema
- `toJson()`/`fromJson()` for serialization - cache doesn't parse the plan

---

### 3.3 ExecutionPlanCache (New)

```java
/**
 * Cache for execution plans.
 * Treats ExecutionPlan as opaque - doesn't inspect its contents.
 */
public class ExecutionPlanCache {
    private final Path cacheDir;  // <handbook>/plans/
    
    public ExecutionPlanCache(Path handbookPath) {
        this.cacheDir = handbookPath.resolve("plans");
        Files.createDirectories(cacheDir);
    }
    
    /**
     * Look up a cached plan by schema.
     */
    public Optional<ExecutionPlan> lookup(PromptSchema schema) {
        Path planFile = cacheDir.resolve(schema.getCacheKey() + ".json");
        if (!Files.exists(planFile)) {
            return Optional.empty();
        }
        
        String fileContent = Files.readString(planFile);
        CachedPlanFile cached = CachedPlanFile.fromJson(fileContent);
        return Optional.of(ExecutionPlan.fromJson(cached.getPlanJson()));
    }
    
    /**
     * Store a plan in the cache.
     */
    public void store(PromptSchema schema, ExecutionPlan plan) {
        CachedPlanFile cached = new CachedPlanFile(
            schema.getCacheKey(),
            Instant.now(),
            plan.toJson()  // Plan serializes itself
        );
        
        Path planFile = cacheDir.resolve(schema.getCacheKey() + ".json");
        Files.writeString(planFile, cached.toJson());
    }
}

/**
 * Internal: Structure of a cached plan file.
 */
class CachedPlanFile {
    private String psk;
    private Instant createdAt;
    private String planJson;  // The plan as a JSON string
}
```

---

### 3.4 SchemaAwarePlanGenerator (New)

```java
/**
 * Generates parameterized execution plans from prompt + schema.
 */
public class SchemaAwarePlanGenerator {
    private final OneMcp oneMcp;
    
    /**
     * Generate a new execution plan.
     * The plan uses placeholders like {{params.year}} for reusability.
     */
    public ExecutionPlan createPlan(String prompt, PromptSchema schema) {
        // 1. Load prompt template
        PromptTemplate template = oneMcp.promptRepository()
            .get("plan-generation-schema-aware");
        
        // 2. Build context
        Map<String, Object> context = Map.of(
            "prompt", prompt,
            "schema", schema,
            "operations", getAvailableOperations()
        );
        
        // 3. Call LLM
        String response = oneMcp.llmClient().generate(
            template.render(context), List.of(), false, null);
        
        // 4. Parse response into ExecutionPlan
        String jsonContent = StringUtility.extractSnippet(response, "json");
        return ExecutionPlan.fromJson(jsonContent);
    }
}
```

---

## 4. Cache File Structure

```
<handbook>/
  └── plans/
      ├── summarize-sale-amount_year-group_category.json
      ├── search-sale-amount_date.json
      └── ...
```

**File format**:
```json
{
  "psk": "summarize-sale-amount_year-group_category",
  "created_at": "2025-01-15T10:00:00Z",
  "plan": {
    "start_node": {
      "vars": { "year": "{{params.year}}" },
      "route": "query_sales"
    },
    "query_sales": {
      "operation": "querySalesData",
      "input": {
        "filter": [
          { "field": "date.year", "operator": "equals", "value": "{{params.year}}" }
        ],
        "aggregates": [
          { "field": "sale.amount", "function": "{{params.amount.aggregate}}" }
        ]
      },
      "route": "summary"
    },
    "summary": {
      "completed": true,
      "vars": { "answer": "$.query_sales.data" }
    }
  }
}
```

---

## 5. Example Walkthrough

### 5.1 First Request (Cache Miss)

**User prompt**: `"Show me total sales in 2024 by category"`

```
Step 1: normalizePrompt(prompt)
        ↓
        PromptSchema {
          action: "summarize",
          entities: ["sale"],
          groupBy: ["category"],
          params: { "year": "2024", "amount": { "aggregate": "sum" } },
          cacheKey: "summarize-sale-amount_year-group_category"
        }

Step 2: cache.lookup(schema)
        ↓
        Optional.empty()  // CACHE MISS

Step 3: generator.createPlan(prompt, schema)
        ↓
        ExecutionPlan (with {{params.year}} placeholders)

Step 5: cache.store(schema, plan)
        ↓
        Writes: <handbook>/plans/summarize-sale-amount_year-group_category.json

Step 4: plan.execute(schema, registry)
        ↓
        Internally: hydrate {{params.year}} → "2024"
        Internally: run ExecutionPlanEngine
        Return: "Total sales in 2024 by category: Electronics $45,230..."
```

### 5.2 Second Request (Cache Hit)

**User prompt**: `"Show me total sales in 2023 by category"`

```
Step 1: normalizePrompt(prompt)
        ↓
        PromptSchema {
          ...
          params: { "year": "2023", ... },
          cacheKey: "summarize-sale-amount_year-group_category"  // SAME KEY!
        }

Step 2: cache.lookup(schema)
        ↓
        Optional.of(ExecutionPlan)  // CACHE HIT ✓

        (Skip steps 3 and 5)

Step 4: plan.execute(schema, registry)
        ↓
        Internally: hydrate {{params.year}} → "2023"
        Return: "Total sales in 2023 by category: Electronics $41,500..."
```

**No LLM call for plan generation on the second request!**

---

## 6. Implementation Tasks

### 6.1 Task: ExecutionPlan Class

**File**: `packages/server/src/main/java/com/gentoro/onemcp/cache/ExecutionPlan.java`

- [ ] Create class with private `JsonNode planNode`
- [ ] Implement `static fromJson(String json)` factory
- [ ] Implement `static fromNode(JsonNode node)` factory
- [ ] Implement `String toJson()` serialization
- [ ] Implement `String execute(PromptSchema schema, OperationRegistry registry)`
- [ ] Implement private `hydrate(JsonNode, Map<String, Object>)` method
  - [ ] Recursively traverse JSON
  - [ ] Find `{{params.X}}` patterns
  - [ ] Replace with values from params map
  - [ ] Handle nested paths: `{{params.amount.aggregate}}`
- [ ] Implement private `formatResult(JsonNode)` method
- [ ] Write unit tests

### 6.2 Task: ExecutionPlanCache Class

**File**: `packages/server/src/main/java/com/gentoro/onemcp/cache/ExecutionPlanCache.java`

- [ ] Create class with `Path cacheDir` constructor parameter
- [ ] Create `CachedPlanFile` inner class (psk, createdAt, planJson)
- [ ] Implement `Optional<ExecutionPlan> lookup(PromptSchema schema)`
  - [ ] Build path: `<cacheDir>/<schema.cacheKey>.json`
  - [ ] If exists: read, parse, return `Optional.of(ExecutionPlan.fromJson(...))`
  - [ ] If not exists: return `Optional.empty()`
- [ ] Implement `void store(PromptSchema schema, ExecutionPlan plan)`
  - [ ] Build `CachedPlanFile` with metadata
  - [ ] Write to `<cacheDir>/<schema.cacheKey>.json`
- [ ] Add logging for cache hit/miss
- [ ] Write unit tests

### 6.3 Task: SchemaAwarePlanGenerator Class

**File**: `packages/server/src/main/java/com/gentoro/onemcp/cache/SchemaAwarePlanGenerator.java`

- [ ] Create class with `OneMcp` dependency
- [ ] Implement `ExecutionPlan createPlan(String prompt, PromptSchema schema)`
  - [ ] Load prompt template `plan-generation-schema-aware`
  - [ ] Build context with prompt, schema, available operations
  - [ ] Call LLM
  - [ ] Parse response JSON
  - [ ] Return `ExecutionPlan.fromJson(...)`
- [ ] Add retry logic (up to 3 attempts on parse failure)
- [ ] Write unit tests

### 6.4 Task: Prompt Template

**File**: `packages/server/src/main/resources/prompts/plan-generation-schema-aware.yaml`

- [ ] Create system prompt explaining:
  - [ ] Placeholder syntax: `{{params.<field>}}`
  - [ ] How to map schema structure to DAG nodes
  - [ ] Available operations from KnowledgeBase
- [ ] Include examples of parameterized plans
- [ ] Add user prompt section with schema and original prompt

### 6.5 Task: Update PromptSchema

**File**: `packages/server/src/main/java/com/gentoro/onemcp/cache/PromptSchema.java`

- [ ] Ensure `generateCacheKey()` is called after normalization
- [ ] Verify cacheKey excludes param values (only uses keys)
- [ ] Add getter for `cacheKey`

### 6.6 Task: Update OrchestratorService

**File**: `packages/server/src/main/java/com/gentoro/onemcp/orchestrator/OrchestratorService.java`

- [ ] Add dependencies:
  - [ ] `PromptSchemaNormalizer` (exists)
  - [ ] `ExecutionPlanCache` (new)
  - [ ] `SchemaAwarePlanGenerator` (new)
- [ ] Move normalization from background to main path
- [ ] Implement new flow:
  ```java
  PromptSchema schema = normalizePrompt(prompt);
  Optional<ExecutionPlan> cached = cache.lookup(schema);
  ExecutionPlan plan = cached.orElseGet(() -> {
      ExecutionPlan newPlan = generator.createPlan(prompt, schema);
      cache.store(schema, newPlan);
      return newPlan;
  });
  return plan.execute(schema, registry);
  ```
- [x] Remove legacy EXTRACT→PLAN flow (normalization replaces entity extraction)
- [x] Add cache hit/miss logging  
- [ ] Add telemetry (timing stats)
- [ ] Write integration tests

---

## 7. File Summary

### New Files

| File | Purpose |
|------|---------|
| `cache/ExecutionPlan.java` | First-class plan object with `execute()` |
| `cache/ExecutionPlanCache.java` | Cache storage/retrieval |
| `cache/SchemaAwarePlanGenerator.java` | Plan generation from schema |
| `prompts/plan-generation-schema-aware.yaml` | LLM prompt template |

### Modified Files

| File | Changes |
|------|---------|
| `orchestrator/OrchestratorService.java` | New flow using major operations |
| `cache/PromptSchema.java` | Ensure cacheKey generation |
| `cache/PromptSchemaNormalizer.java` | Move to main path (minor) |

---

## 8. Implementation Order

**Recommended sequence**:

1. **ExecutionPlan class** - Core object, needed by everything else
2. **ExecutionPlanCache** - Simple file I/O, depends on ExecutionPlan
3. **Hydration logic** - Inside ExecutionPlan.execute()
4. **SchemaAwarePlanGenerator** - Most complex (LLM call)
5. **Prompt template** - For the generator
6. **OrchestratorService** - Wire everything together

---

## 9. Scope

### In Scope
- Single-step PromptSchema support
- All 5 major operations
- First-class `ExecutionPlan` object
- File-based cache

### Out of Scope (Future)
- Multi-step workflows
- Cache invalidation
- Cache warming

---

## 10. Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1 | 2025-01 | AI | Initial draft |
| 0.2 | 2025-01 | AI | Restructured around 5 major operations |
| 0.3 | 2025-01 | AI | Added first-class ExecutionPlan object, merged tasks |
