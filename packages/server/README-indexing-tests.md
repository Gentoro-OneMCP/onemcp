### Indexing package — tests and contributor guide

This document explains the indexing subsystem under `com.gentoro.onemcp.indexing`, the structure of its test suite, how to run tests locally, and how to extend them when adding features or new backends.

#### What the indexing subsystem does

- Transforms handbook content (APIs + Markdown docs) into a unified, backend‑agnostic set of knowledge nodes.
- Supports multiple storage backends via `GraphDriver` (in‑memory, Neo4j, ArangoDB, etc.).
- Retrieves nodes by context using `(entity, operations)` tuples.

Key classes:
- `HandbookGraphService` — end‑to‑end service: collects handbook assets, chunks Markdown via `MarkdownChunker`, converts OpenAPI to normalized nodes via `OpenApiToNodes`, and persists them via `GraphDriver`.
- `GraphDriver` — SPI implemented per backend. Reference implementation: `InMemoryGraphDriver`.
- `GraphNodeRecord` — canonical node schema published by the service and consumed by drivers.
- `GraphContextTuple` — retrieval input describing the desired entity and optional operation kinds.
- `MarkdownChunker` — parses YAML front matter (`entities`, `operations`) and splits Markdown by strategy: `PARAGRAPH`, `HEADING`, `SLIDING_WINDOW`.
- `OpenApiToNodes` — converts `Api` + `OpenAPI` to nodes: API docs, per‑operation docs, input/output schemas, examples.

#### Running tests

From the repository root, you can run specific test classes using your IDE or Maven/Gradle if configured. In this repository, the JetBrains tool integration can run individual files as shown below. Common JUnit 5 conventions apply.

If running via Maven:
```
mvn -f packages/server test -Dtest=com.gentoro.onemcp.indexing.*Test
```

#### Test coverage overview

1. Core models
   - `GraphNodeRecordTest`: round‑trip `toMap`/`fromMap`, null/empty handling for lists.
   - `GraphContextTupleTest`: immutability and unmodifiable operations list.

2. Reference backend (in‑memory)
   - `InMemoryGraphDriverTest`: lifecycle (`initialize`, `clearAll`, `shutdown`), `upsertNodes` idempotency by key, `deleteNodesByKeys`, and `queryByContext` semantics for entity‑only and entity+operation matches, including multi‑tuple OR behavior.

3. Markdown chunking
   - `MarkdownChunkerTest`: validates the three strategies and edge cases:
     - Paragraph grouping and hard‑splitting long paragraphs
     - Heading‑based splits with size control for large sections
     - Sliding window windows/overlap math (including `overlap >= window`)
     - YAML front matter extraction of `entities` and `operations` and removal from content

4. OpenAPI mapping
   - `OpenApiToNodesTest`: constructs a minimal `OpenAPI` with tags, paths, request/response schemas; verifies generated node types, fields, and content formats.

5. Service integration
   - `HandbookGraphServiceTest`: indexes real Markdown files in a temporary handbook directory into an in‑memory driver; verifies `clearOnStartup`, chunking strategy configuration, and retrieval by context.
   - Uses a lightweight `TestMcp` subclass of `OneMcp` to inject a custom `Configuration` and `Handbook` without full app initialization. This avoids inline bytecode mocking issues on newer JDKs.

#### Conventions and best practices

- Prefer deterministic, local fixtures (temporary files/directories) over network calls.
- Keep node keys stable and explicit in tests to assert idempotency.
- When adding a new backend driver, add a dedicated test class mirroring `InMemoryGraphDriverTest` semantics to ensure consistent behavior.
- When modifying chunking rules, expand `MarkdownChunkerTest` with focused cases covering splitting logic and front matter interplay.
- When expanding OpenAPI support, add targeted cases to `OpenApiToNodesTest` for new spec features (e.g., multiple responses, request params), keeping assertions on node fields and formats.

#### Extending the suite

When you add new capabilities, follow this checklist:
- Does it affect the canonical node schema? Add/adjust tests in `GraphNodeRecordTest` and update JavaDoc in code.
- Does it change the context matching semantics? Update the in‑memory driver test and ensure other drivers conform.
- Does it add configuration knobs? Extend `HandbookGraphServiceTest` to verify they are respected.

#### Troubleshooting

- Mockito inline mocking on newer JDKs (e.g., 21+ / 24) can fail for some classes. Prefer concrete test doubles (small subclasses or simple interface impls) instead of mocking final classes or complex constructors.
- If logging is verbose in tests, adjust `packages/server/src/test/resources/logback-test.xml` levels.
