Indexing
========

Overview
--------
- Builds a relationship graph from the Handbook so downstream features (retrieval, entity context, operations lookup) can query it through the graph drivers.
- Runs inside the server: `OneMcp.initialize()` loads the Handbook, checks `graph.indexing.enabled`, and invokes the indexing service before HTTP endpoints start.

Prerequisites
-------------
- **Graph database**: ArangoDB is the default driver. Start it locally or point to a managed instance. Example:
  `docker run -e ARANGO_ROOT_PASSWORD=test123 -p 8529:8529 -d arangodb:latest`
- **LLM profile**: Indexing reuses the active LLM profile configured under `llm.active-profile`. Ensure API keys/environment variables are in place before launching the server.
- **Handbook content**: The Handbook directory configured via `handbook.location` must be accessible; indexing walks the same documents the KnowledgeBase ingests.

Configuration
-------------
- All toggles live under `graph.indexing` in `application.yaml`:

```yaml
graph:
  indexing:
    enabled: ${env:GRAPH_INDEXING_ENABLED:-false}
    clearOnStartup: ${env:GRAPH_INDEXING_CLEAR_ON_STARTUP:-true}
    driver: ${env:GRAPH_INDEXING_DRIVER:-arangodb}
    arangodb:
      enabled: ${env:ARANGODB_ENABLED:-true}
      host: ${env:ARANGODB_HOST:-localhost}
      port: ${env:ARANGODB_PORT:-8529}
      user: ${env:ARANGODB_USER:-root}
      password: ${env:ARANGODB_PASSWORD:-test123}
```

See [`application.yaml`](https://github.com/Gentoro-OneMCP/onemcp/blob/main/packages/server/src/main/resources/application.yaml#L68-L89) for the full configuration.

- Set the same keys via environment variables in CI or local shells; they use the `GRAPH_*` and `ARANGODB_*` prefixes shown above.
- `clearOnStartup` wipes previously indexed data each boot, ensuring schema drift or handbook edits do not create duplicates.

Enabling Indexing
-----------------
1. **Start dependencies**  
   - Launch ArangoDB and confirm port `8529` is reachable.  
   - Export credentials (e.g., `export ARANGODB_PASSWORD=test123`).
2. **Configure the server**  
   - Turn on the feature: `export GRAPH_INDEXING_ENABLED=true`.  
   - Opt into the Arango driver: `export GRAPH_INDEXING_DRIVER=arangodb` (Arango stays enabled by default via `ARANGODB_ENABLED`, override it only if you need to disable the driver).  
   - Optional: `export GRAPH_INDEXING_CLEAR_ON_STARTUP=true` for clean re-runs.
3. **Launch OneMCP**  
   - From `packages/server`, run `mvn package` (first time) and start the server (`java -jar target/onemcp-1.0-SNAPSHOT.jar`).  
   - During startup, the console shows `Graph indexing enabled via configuration`, indicating the module is running.

Operational Notes
-----------------
- **When indexing runs**: happens automatically during server boot, before Jetty starts, so the graph is ready by the time the HTTP + MCP endpoints are available.
- **Manual re-index**: Restart the server after editing Handbook content or LLM prompts. With `clearOnStartup=true`, each restart rebuilds the graph from scratch.
- **Partial updates**: Not supported; re-run the full indexing pass whenever the source material changes.
- **Driver selection**: Only Arango is wired up today; set `graph.indexing.driver` to another key once a new driver is added without touching the calling code.

Observability & Logs
--------------------
- All indexing artifacts land under `packages/server/logs` with descriptive prefixes:
  - `*-llm-prompt-...log`: prompt payloads sent to the LLM.
  - `*-llm-response-...log`: verbatim responses for auditing.
  - `*-llm-graph-...log`: structured snapshot of the nodes and relationships sent to the driver.
  - `*-llm-parse-error-...log`: raw payloads plus stack traces whenever parsing fails.
- Use these logs to inspect what was indexed, replay prompts, or share repros with the infra team.

Querying the Graph
------------------
- Reference implementation: [`GraphQueryScript`](https://github.com/Gentoro-OneMCP/onemcp/blob/main/packages/server/src/main/java/com/gentoro/onemcp/scripts/GraphQueryScript.java) shows the full lifecycle (parsing a JSON context, bootstrapping `OneMcp`, constructing `GraphQueryService`, and printing JSON results). Check the source for a copy-pasteable example when building new tooling.
- [`GraphQueryService`](https://github.com/Gentoro-OneMCP/onemcp/blob/main/packages/server/src/main/java/com/gentoro/onemcp/indexing/GraphQueryService.java) wraps the configured `GraphQueryDriver` (default Arango) so server flows can request entity + operation context without dealing with AQL.
- OneMCP creates it on demand in components such as scripts or orchestrator steps:

```java
public class GraphQueryService implements AutoCloseable {
  public List<QueryResult> query(QueryRequest request) { ... }
}
```
- To use it inside server code:
  1. Inject the running `OneMcp` instance.
  2. Construct a `GraphQueryService` (optionally pass a custom driver for tests).
  3. Build a `QueryRequest` with one or more `ContextItem` entries (entity name + desired operation categories/tags).
  4. Call `query(request)`; the service initializes the driver if needed, issues the optimized traversal (see [`entity-context-query.aql`](https://github.com/Gentoro-OneMCP/onemcp/blob/main/packages/server/src/main/resources/aql/entity-context-query.aql)), and returns `QueryResult` objects with entity details, fields, operation metadata, examples, and docs.
  5. Always call `close()` or use try-with-resources so the underlying driver can release pooled connections.
- Configuration mirrors indexing: `graph.query.driver` overrides the driver used at runtime; if absent, it falls back to `graph.indexing.driver`, so keeping both in sync is the easiest path.
- For CLI/testing flows, run [`test-graph-query.sh`](https://github.com/Gentoro-OneMCP/onemcp/blob/main/scripts/server/test-graph-query.sh) to bootstrap `GraphQueryScript`, which wires the same service end-to-end against the shaded server jar.

Verification & Troubleshooting
------------------------------
- Confirm the Arango graph via the admin UI at `http://localhost:8529` (database named after the handbook).
- Run [`test-graph-query.sh`](https://github.com/Gentoro-OneMCP/onemcp/blob/main/scripts/server/test-graph-query.sh) to execute a smoke query against the graph driver; it compiles the shaded jar if needed and uses the same driver settings.
- If indexing is skipped:
  - Check that `GRAPH_INDEXING_ENABLED` **and** `ARANGODB_ENABLED` are true.
  - Validate the configured credentials by logging into Arango with the same host/user/password.
  - Ensure the selected LLM profile is reachable; without it, indexing falls back to rule-based extraction, which still requires the graph driver to be initialized.

Extending
---------
- To introduce another backend (e.g., Neo4j), add a driver, expose its configuration under `graph.indexing.<driverKey>`, then point `graph.indexing.driver` to that key. The service performs the rest of the orchestration, so existing startup logic continues to work unchanged.

Quick Checklist
---------------
- [ ] Handbook path resolves
- [ ] LLM active profile configured
- [ ] Graph DB reachable and credentials set
- [ ] `GRAPH_INDEXING_ENABLED=true`
- [ ] Driver-specific env vars exported
- [ ] Server restart triggered to kick off indexing

