# OneMCP

This module contains the OneMCP service and its tests.

## Knowledge Base
The agent builds a lightweight Knowledge Base from a Foundation directory on startup. It scans:
- docs/*.md (and .mdx)
- tests/** (all files)
- feedback/* (all files)
- openapi/*.(yaml|yml|json)

Each discovered item becomes an entry addressable via an abstract URI: kb://<type>/<path>. The service maps these to original file:// paths or in-memory mem:// resources produced by OpenAPI SDK doc generation.

Hints: For each entry a short hint is generated. If knowledgeBase.hint.useAi is true, the agent will call the configured LLM to produce a concise sentence; otherwise, it truncates the content to the configured length.

Configuration (application.yaml):
- knowledgeBase.foundation.dir (env: FOUNDATION_DIR) — Foundation root directory. Must exist.
- knowledgeBase.hint.useAi — Use LLM to generate hints (default: false).
- knowledgeBase.hint.size — Max hint length in characters.

Caching: The computed entries and a signature are persisted to foundation/state/knowledge-base-state.json so subsequent startups can restore quickly when the Foundation directory has not changed.

## Running tests

The project separates unit tests from integration tests using Maven Surefire and Failsafe.

- Unit tests match: `**/*Test.java`
- Integration tests match: `**/*IntegrationTest.java`

### Commands
- Unit tests only (default):
  - `mvn test`
- Run unit + integration tests:
  - `mvn verify`
- Run only integration tests (skip unit tests):
  - `mvn -DskipTests -DskipITs=false verify`
- Skip integration tests even during `verify`:
  - `mvn -DskipITs verify`

Notes:
- Surefire runs during the `test` phase and is configured to exclude `*IntegrationTest.java`.
- Failsafe runs during the `integration-test` and `verify` phases and is configured to include only `*IntegrationTest.java`.

### Environment for integration tests
Some integration tests call external services and may require them to be running locally:

- TypeScript Runtime service used by `TypescriptRuntimeClientIntegrationTest`:
  - Base URL is taken from environment variable `TS_RUNTIME_URL` (default: `http://localhost:7070`).
  - There is a reference implementation under `src/typescript-runtime/` in this repository which you can start according to its README.

If these services are not available, you may see connection-related test failures in the integration test suite. You can either start the services or skip ITs as shown above.
