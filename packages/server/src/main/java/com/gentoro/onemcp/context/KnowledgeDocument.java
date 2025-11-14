package com.gentoro.onemcp.context;

/**
 * Immutable knowledge-base document.
 *
 * <p>Each document represents a single Markdown resource loaded from the configured knowledge base
 * directory. Document identifiers use a stable URI with the scheme {@code kb:///...}, where the
 * path is the file path relative to the knowledge base root.
 */
public record KnowledgeDocument(
    /**
     * Canonical document URI using the scheme {@code kb:///path/to/file.md}. The path component is
     * the file path relative to the configured knowledge base root directory.
     */
    String uri,
    /** Full UTF-8 file content. */
    String content) {}
