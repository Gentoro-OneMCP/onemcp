/**
 * Knowledge base module.
 *
 * <p>This package provides a lightweight knowledge repository that indexes Markdown files under a
 * configured root directory and exposes them as immutable {@link
 * com.gentoro.onemcp.context.KnowledgeDocument} instances. The main entry point is {@link
 * com.gentoro.onemcp.context.KnowledgeBase} which offers simple retrieval operations by exact URI
 * or URI prefix.
 *
 * <h2>URI scheme</h2>
 *
 * <p>Each document is identified by a stable URI that uses the {@code kb} scheme with three leading
 * slashes, e.g., {@code kb:///README.md}. The path component is the file path relative to the
 * configured knowledge base root directory.
 *
 * <h2>Configuration</h2>
 *
 * <ul>
 *   <li>{@code context.storage.dir}: absolute path to the directory containing the Markdown
 *       documents to be indexed.
 * </ul>
 */
package com.gentoro.onemcp.context;
