package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.indexing.docs.Chunk;
import com.gentoro.onemcp.indexing.docs.EntityExtractor;
import com.gentoro.onemcp.indexing.docs.EntityMatch;
import com.gentoro.onemcp.indexing.docs.SemanticMarkdownChunker;
import com.gentoro.onemcp.indexing.driver.memory.InMemoryGraphDriver;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import com.gentoro.onemcp.indexing.openapi.OpenApiToNodes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * End-to-end v2 graph service to index Handbook content and retrieve nodes by context.
 *
 * <p>Supports multiple backends via GraphDriver. Default driver is in-memory unless a custom driver
 * is provided.
 */
public class HandbookGraphService implements AutoCloseable {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(HandbookGraphService.class);

  private final OneMcp oneMcp;
  private final GraphDriver driver;
  private final OpenApiToNodes openApiToNodes;

  public HandbookGraphService(OneMcp oneMcp) {
    this(oneMcp, resolveDriver(oneMcp));
  }

  public HandbookGraphService(OneMcp oneMcp, GraphDriver driver) {
    this.oneMcp = oneMcp;
    this.driver = Objects.requireNonNull(driver, "driver");

    int windowSize = 500;
    try {
      windowSize =
          oneMcp.configuration().getInt("graph.indexing.chunking.markdown.windowSizeTokens", 500);
    } catch (Exception ignored) {
    }

    int overlap = 64;
    try {
      overlap = oneMcp.configuration().getInt("graph.indexing.chunking.markdown.overlapTokens", 64);
    } catch (Exception ignored) {
    }

    this.openApiToNodes = new OpenApiToNodes();
  }

  public void initialize() {
    driver.initialize();
  }

  public void clearAll() {
    driver.clearAll();
  }

  /** Index all handbook APIs and docs into the knowledge graph. */
  public void indexHandbook() {
    ensureReady();
    Handbook handbook = oneMcp.handbook();

    // Clear if configured
    // v2 config key
    boolean clear = oneMcp.configuration().getBoolean("graph.indexing.clearOnStartup", true);
    if (clear) driver.clearAll();

    List<GraphNodeRecord> batch = new ArrayList<>();

    // APIs (OpenAPI + Agent mapping)
    for (Api api : handbook.agent().getApis()) {
      try {
        List<GraphNodeRecord> apiNodes = openApiToNodes.buildNodes(api);
        batch.addAll(apiNodes);
      } catch (Exception e) {
        log.warn("Failed to index API '{}': {}", api.getSlug(), e.getMessage(), e);
      }
    }

    // Docs (markdown chunking with front matter)
    Map<String, String> docs = handbook.documentation();
    for (Map.Entry<String, String> entry : docs.entrySet()) {
      String relPath = entry.getKey();
      String content = entry.getValue();
      try {
        Path docPath = handbook.location().resolve(relPath);
        // Ensure content matches file system by writing temp file if needed
        // Prefer FS when exists; else chunk in-memory by temp file
        if (!Files.exists(docPath)) {
          // fallback: create ephemeral content
          Path tmp = Files.createTempFile("handbook_doc_", ".md");
          Files.writeString(tmp, content);
          addDocChunks(tmp, batch);
          Files.deleteIfExists(tmp);
        } else {
          addDocChunks(docPath, batch);
        }
      } catch (Exception e) {
        log.warn("Failed to index doc '{}': {}", relPath, e.getMessage(), e);
      }
    }

    // Persist
    driver.upsertNodes(batch);
    log.info("Indexed {} nodes into graph backend '{}'", batch.size(), driver.getDriverName());
  }

  private void addDocChunks(Path file, List<GraphNodeRecord> batch) {
    // --- 1) Setup chunker (minTokens, maxTokens, overlapTokens)
    SemanticMarkdownChunker chunker =
        new SemanticMarkdownChunker(
            150, // min tokens
            450, // max tokens
            40 // overlap tokens (approx)
            );

    List<Chunk> chunks;
    try {
      // Chunk the file
      chunks = chunker.chunkFile(file);
      log.info("Produced {} chunks:", chunks.size());
      for (var c : chunks) {
        log.trace(" - {} | {} | len={}", c.id(), c.sectionPath(), c.content().length());
      }
    } catch (IOException e) {
      throw new IoException(
          "Failed while attempting to generate documentation chunks for " + file, e);
    }

    EntityExtractor extractor =
        new EntityExtractor(
            oneMcp,
            oneMcp.handbook().apis().values().stream()
                .flatMap(api -> api.getEntities().stream())
                .toList());

    // Extract entities for each chunk
    for (var c : chunks) {
      var res = extractor.extract(c);
      log.info("Chunk: {} -> matches: {}", c.id(), res.getMatches().size());
      for (var m : res.getMatches()) {
        log.trace("   - {} ({}}) : {}%n", m.getEntity(), m.getConfidence(), m.getReason());
      }
      GraphNodeRecord n =
          new GraphNodeRecord(
                  key("doc", file.toString(), Integer.toString(c.content().hashCode())),
                  KnowledgeNodeType.DOCS_CHUNK)
              .setDocPath(c.fileName())
              .setEntities(res.getMatches().stream().map(EntityMatch::getEntity).toList())
              .setOperations(Collections.emptyList())
              .setContent(c.content())
              .setContentFormat("markdown");
      batch.add(n);
    }
  }

  public List<Map<String, Object>> retrieveByContext(List<GraphContextTuple> context) {
    ensureReady();
    return driver.queryByContext(context);
  }

  private void ensureReady() {
    if (!driver.isInitialized()) driver.initialize();
  }

  private static String resolveHandbookName(OneMcp oneMcp) {
    try {
      return oneMcp.handbook().name();
    } catch (Exception e) {
      return "default";
    }
  }

  private static String key(String... parts) {
    return String.join("|", parts);
  }

  private static GraphDriver resolveDriver(OneMcp oneMcp) {
    String handbookName = resolveHandbookName(oneMcp);
    String desired = oneMcp.configuration().getString("graph.driver", "in-memory");
    log.trace("Resolving graph driver for handbook '{}' (desired '{}')", handbookName, desired);
    try {
      // Discover providers via ServiceLoader
      java.util.ServiceLoader<com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider> loader =
          java.util.ServiceLoader.load(
              com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider.class);
      for (com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider p : loader) {
        if (p.id().equalsIgnoreCase(desired) && p.isAvailable(oneMcp)) {
          return p.create(oneMcp, handbookName);
        }
      }
      // Fallback: try an in-memory provider if present
      for (com.gentoro.onemcp.indexing.driver.spi.GraphDriverProvider p : loader) {
        if (p.id().equalsIgnoreCase("in-memory")) {
          return p.create(oneMcp, handbookName);
        }
      }
    } catch (Exception ignored) {
      // ignore and fallback below
    }
    // Absolute fallback: direct in-memory instance
    return new InMemoryGraphDriver(handbookName);
  }

  @Override
  public void close() {
    driver.shutdown();
  }
}
