/**
 * Indexing package — converts handbook assets (APIs and Markdown docs) into a backend‑agnostic
 * knowledge graph and provides context retrieval.
 *
 * <h2>Key components</h2>
 *
 * <ul>
 *   <li>{@link com.gentoro.onemcp.indexing.HandbookGraphService} — end‑to‑end service that reads
 *       the current {@link com.gentoro.onemcp.handbook.Handbook} (APIs + docs), chunks Markdown,
 *       maps OpenAPI operations to normalized nodes, and persists via {@link
 *       com.gentoro.onemcp.indexing.GraphDriver}.
 *   <li>{@link com.gentoro.onemcp.indexing.GraphDriver} — unified driver interface implemented by
 *       backends like {@code in-memory}, Neo4j, ArangoDB. It supports lifecycle, clearing, upserts,
 *       and contextual queries.
 *   <li>{@link com.gentoro.onemcp.indexing.GraphNodeRecord} — canonical node representation. Fields
 *       are backend‑agnostic and cover API docs, operation docs, input/output schemas, examples,
 *       and free‑form docs chunks.
 *   <li>{@link com.gentoro.onemcp.indexing.docs.MarkdownChunker} — parses optional YAML front
 *       matter (entities/operations) and chunks Markdown using one of the strategies: PARAGRAPH,
 *       HEADING, SLIDING_WINDOW. Chunks inherit extracted entities/operations.
 *   <li>{@link com.gentoro.onemcp.indexing.openapi.OpenApiToNodes} — transforms the Agent/API model
 *       (with OpenAPI) into normalized nodes: API documentation; per‑operation documentation,
 *       input/output schemas; examples.
 * </ul>
 *
 * <h2>Driver SPI</h2>
 *
 * Backends register via {@link com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider} using
 * Java ServiceLoader. {@link com.gentoro.onemcp.indexing.HandbookGraphService} resolves a driver by
 * configuration key {@code indexing.graph.driver}. If unavailable, it falls back to the in‑memory driver.
 *
 * <h2>Context retrieval</h2>
 *
 * Consumers call {@link
 * com.gentoro.onemcp.indexing.HandbookGraphService#retrieveByContext(java.util.List)} with {@link
 * com.gentoro.onemcp.indexing.GraphContextTuple}s. A node matches if it shares at least one entity
 * with any tuple. If the node has operations, they must intersect the tuple’s operations for the
 * matched entity; otherwise entity‑only nodes always match.
 *
 * <h2>Testing</h2>
 *
 * See {@code README-indexing-tests.md} in the server package for guidance on running and extending
 * tests. The in‑memory driver serves as the reference semantics for contextual matching.
 */
package com.gentoro.onemcp.indexing;
