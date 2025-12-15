package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.handbook.Handbook;
import com.gentoro.onemcp.handbook.model.agent.Api;
import com.gentoro.onemcp.indexing.docs.Chunk;
import com.gentoro.onemcp.indexing.docs.EntityExtractor;
import com.gentoro.onemcp.indexing.docs.EntityMatch;
import com.gentoro.onemcp.indexing.docs.SemanticMarkdownChunker;
import com.gentoro.onemcp.indexing.docs.Tokenizer;
import com.gentoro.onemcp.indexing.driver.memory.InMemoryGraphDriver;
import com.gentoro.onemcp.indexing.model.KnowledgeNodeType;
import com.gentoro.onemcp.indexing.openapi.OpenApiToNodes;
import com.gentoro.onemcp.indexing.GraphValidationService.ValidationReport;
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
  private final int defaultMinTokens;
  private final int defaultMaxTokens;
  private final int defaultOverlapTokens;
  private final boolean adaptiveChunking;

  public HandbookGraphService(OneMcp oneMcp) {
    this(oneMcp, resolveDriver(oneMcp));
  }

  public HandbookGraphService(OneMcp oneMcp, GraphDriver driver) {
    this.oneMcp = oneMcp;
    this.driver = Objects.requireNonNull(driver, "driver");

    int windowSize = 500;
    try {
      windowSize =
          oneMcp.configuration().getInt("indexing.graph.chunking.markdown.windowSizeTokens", 500);
    } catch (Exception ignored) {
    }

    int overlap = 64;
    try {
      overlap = oneMcp.configuration().getInt("indexing.graph.chunking.markdown.overlapTokens", 64);
    } catch (Exception ignored) {
    }

    // Calculate default min/max tokens from windowSize (min = 30% of window, max = 90% of window)
    this.defaultMinTokens = Math.max(1, (int) (windowSize * 0.3));
    this.defaultMaxTokens = Math.max(this.defaultMinTokens, (int) (windowSize * 0.9));
    this.defaultOverlapTokens = Math.max(0, overlap);

    // Enable adaptive chunking by default (can be disabled via config)
    this.adaptiveChunking =
        oneMcp.configuration().getBoolean("indexing.graph.chunking.markdown.adaptive", true);

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
    boolean clear = oneMcp.configuration().getBoolean("indexing.graph.clearOnStartup", true);
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

    // Calculate optimal chunking parameters based on total documentation size
    ChunkingParams chunkingParams = calculateOptimalChunkingParams(docs, handbook);
    log.info(
        "Using chunking params: min={}, max={}, overlap={} (adaptive={})",
        chunkingParams.minTokens,
        chunkingParams.maxTokens,
        chunkingParams.overlapTokens,
        adaptiveChunking);

    int totalDocs = docs.size();
    int processedDocs = 0;
    for (Map.Entry<String, String> entry : docs.entrySet()) {
      String relPath = entry.getKey();
      String content = entry.getValue();
      processedDocs++;
      log.debug("Processing doc {}/{}: {}", processedDocs, totalDocs, relPath);
      try {
        Path docPath = handbook.location().resolve(relPath);
        // Ensure content matches file system by writing temp file if needed
        // Prefer FS when exists; else chunk in-memory by temp file
        if (!Files.exists(docPath)) {
          // fallback: create ephemeral content
          Path tmp = Files.createTempFile("handbook_doc_", ".md");
          Files.writeString(tmp, content);
          addDocChunks(tmp, batch, chunkingParams);
          Files.deleteIfExists(tmp);
        } else {
          addDocChunks(docPath, batch, chunkingParams);
        }
      } catch (Exception e) {
        log.warn("Failed to index doc '{}': {}", relPath, e.getMessage(), e);
      }
    }

    // Persist
    driver.upsertNodes(batch);
    log.info("Indexed {} nodes into graph backend '{}'", batch.size(), driver.getDriverName());

    // Run validation if enabled
    boolean runValidation =
        oneMcp.configuration().getBoolean("indexing.graph.validation.runAfterIndexing", false);
    if (runValidation) {
      log.info("Running graph validation after indexing...");
      try {
        GraphValidationService validator = new GraphValidationService(oneMcp, this);
        ValidationReport report = validator.validate(true);
        
        // Save report to file if configured
        String reportPath = saveValidationReport(report, oneMcp);
        if (reportPath != null) {
          log.info("Validation report saved to: {}", reportPath);
        }
        
        // Always log the full report
        if (report.hasErrors()) {
          log.error("Graph validation report:\n{}", report);
        } else if (!report.getWarnings().isEmpty()) {
          log.warn("Graph validation report:\n{}", report);
        } else {
          log.info("Graph validation report:\n{}", report);
        }
      } catch (Exception e) {
        log.warn("Graph validation failed after indexing: {}", e.getMessage(), e);
      }
    } else {
      log.debug(
          "Graph validation not running automatically. "
              + "Set GRAPH_VALIDATION_RUN_AFTER_INDEXING=true to enable automatic validation after indexing.");
    }
  }

  /**
   * Manually trigger graph validation and return the report.
   *
   * @return validation report with errors, warnings, and recommendations
   */
  public GraphValidationService.ValidationReport validateGraph() {
    GraphValidationService validator = new GraphValidationService(oneMcp, this);
    ValidationReport report = validator.validate();
    
    // Save report to file if configured
    String reportPath = saveValidationReport(report, oneMcp);
    if (reportPath != null) {
      log.info("Validation report saved to: {}", reportPath);
    }
    
    return report;
  }

  /**
   * Save validation report to a file if output path is configured.
   *
   * @param report the validation report to save
   * @param oneMcp the OneMcp instance for configuration access
   * @return the path where the report was saved, or null if not saved
   */
  private String saveValidationReport(ValidationReport report, OneMcp oneMcp) {
    try {
      String outputPath = oneMcp.configuration().getString("indexing.graph.validation.reportPath", null);
      if (outputPath == null || outputPath.isBlank()) {
        // Default to logs directory if not configured
        String logsDir = oneMcp.configuration().getString("logging.dir", "logs");
        outputPath = logsDir + "/graph-validation-report-" + System.currentTimeMillis() + ".txt";
      }
      
      // Ensure directory exists
      java.io.File file = new java.io.File(outputPath);
      java.io.File parentDir = file.getParentFile();
      if (parentDir != null && !parentDir.exists()) {
        parentDir.mkdirs();
      }
      
      // Write report to file
      try (java.io.FileWriter writer = new java.io.FileWriter(file)) {
        writer.write(report.toString());
        writer.flush();
      }
      
      return file.getAbsolutePath();
    } catch (Exception e) {
      log.warn("Failed to save validation report to file: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Calculates optimal chunking parameters based on handbook size and entity count.
   *
   * <p>Strategy:
   * <ul>
   *   <li>Small handbooks (&lt; 50k tokens): Larger chunks (600-800 tokens) to minimize LLM calls
   *       and provide more context per chunk during retrieval, helping the LLM better understand
   *       relationships and details
   *   <li>Medium handbooks (50k-200k tokens): Medium chunks (400-600 tokens) for balanced
   *       token efficiency and context richness
   *   <li>Large handbooks (&gt; 200k tokens): Smaller chunks (200-400 tokens) to respect context
   *       window limits while still maintaining sufficient context per chunk
   *   <li>More entities = larger prompt overhead, so slightly reduce chunk size
   * </ul>
   *
   * <p>Benefits of larger chunks:
   * <ul>
   *   <li>Fewer LLM calls during entity extraction (reduces token consumption)
   *   <li>More context per chunk when retrieved during plan generation (improves LLM understanding)
   *   <li>Better preservation of relationships and details within documentation sections
   * </ul>
   */
  private ChunkingParams calculateOptimalChunkingParams(Map<String, String> docs, Handbook handbook) {
    if (!adaptiveChunking) {
      return new ChunkingParams(defaultMinTokens, defaultMaxTokens, defaultOverlapTokens);
    }

    // Calculate total documentation size in tokens
    int totalDocTokens = 0;
    for (String content : docs.values()) {
      totalDocTokens += Tokenizer.estimateTokens(content);
    }

    // Count total entities (affects prompt overhead per chunk)
    int entityCount =
        handbook.apis().values().stream()
            .mapToInt(api -> api.getEntities() != null ? api.getEntities().size() : 0)
            .sum();

    // Base chunk size calculation based on total documentation size
    int targetMaxTokens;
    if (totalDocTokens < 50_000) {
      // Small handbook: use larger chunks to minimize LLM calls during entity extraction
      // and provide richer context per chunk when retrieved during plan generation
      targetMaxTokens = 700;
    } else if (totalDocTokens < 200_000) {
      // Medium handbook: balanced approach between token efficiency and context richness
      targetMaxTokens = 500;
    } else {
      // Large handbook: smaller chunks to respect context window limits while maintaining
      // sufficient context per chunk for effective retrieval
      targetMaxTokens = 350;
    }

    // Adjust for entity count (more entities = larger prompt overhead)
    // Reduce chunk size by ~5% per 10 entities above 10
    if (entityCount > 10) {
      double entityAdjustment = 1.0 - (Math.min(entityCount - 10, 50) / 10.0 * 0.05);
      targetMaxTokens = (int) (targetMaxTokens * entityAdjustment);
    }

    // Ensure reasonable bounds
    targetMaxTokens = Math.max(200, Math.min(800, targetMaxTokens));
    int minTokens = Math.max(100, (int) (targetMaxTokens * 0.3));
    int maxTokens = Math.max(minTokens, (int) (targetMaxTokens * 0.95));

    // Overlap: 10-15% of max tokens, but cap at 100 tokens for efficiency
    int overlapTokens = Math.min(100, (int) (maxTokens * 0.12));

    return new ChunkingParams(minTokens, maxTokens, overlapTokens);
  }

  private void addDocChunks(Path file, List<GraphNodeRecord> batch, ChunkingParams params) {
    // --- 1) Setup chunker (minTokens, maxTokens, overlapTokens)
    SemanticMarkdownChunker chunker =
        new SemanticMarkdownChunker(params.minTokens, params.maxTokens, params.overlapTokens);

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

    // --- 2) Create document node for this file
    String documentKey = key("document", file.toString());
    GraphNodeRecord documentNode =
        new GraphNodeRecord(documentKey, KnowledgeNodeType.DOCUMENT)
            .setDocPath(file.toString())
            .setContentFormat("markdown");
    // Extract entities from all chunks to assign to document (union of all chunk entities)
    EntityExtractor extractor =
        new EntityExtractor(
            oneMcp,
            oneMcp.handbook().apis().values().stream()
                .flatMap(api -> api.getEntities().stream())
                .toList());

    Set<String> documentEntities = new HashSet<>();
    for (var c : chunks) {
      var res = extractor.extract(c);
      documentEntities.addAll(
          res.getMatches().stream().map(EntityMatch::getEntity).toList());
    }
    documentNode.setEntities(new ArrayList<>(documentEntities));
    batch.add(documentNode);
    log.debug("Created document node '{}' with {} entities", documentKey, documentEntities.size());

    // --- 3) Extract entities for each chunk and link to document
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
              .setParentDocumentKey(documentKey)
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
    String desired = oneMcp.configuration().getString("indexing.graph.driver", "in-memory");
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

  /** Holds chunking parameters for a specific indexing run. */
  private static class ChunkingParams {
    final int minTokens;
    final int maxTokens;
    final int overlapTokens;

    ChunkingParams(int minTokens, int maxTokens, int overlapTokens) {
      this.minTokens = minTokens;
      this.maxTokens = maxTokens;
      this.overlapTokens = overlapTokens;
    }
  }
}
