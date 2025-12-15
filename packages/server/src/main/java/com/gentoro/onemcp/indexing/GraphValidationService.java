package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.model.LlmClient;
import com.gentoro.onemcp.utility.StringUtility;
import java.util.*;
import java.util.stream.Collectors;

/**
 * LLM-based validation service for knowledge graph structure and content accuracy.
 *
 * <p>This service uses LLM to verify:
 * <ul>
 *   <li>Graph structure correctness (document nodes, chunk linking, relationships)
 *   <li>Content accuracy (chunks accurately represent source documents)
 *   <li>Entity extraction accuracy (entities match chunk content)
 *   <li>Relationship correctness (parent-child relationships are semantically valid)
 *   <li>Coverage completeness (no important information lost in chunking)
 *   <li>Semantic coherence (chunks maintain meaning and context)
 * </ul>
 *
 * <p>Validation is performed strategically to balance thoroughness with cost:
 * <ul>
 *   <li>Structural checks use deterministic logic (fast, free)
 *   <li>Semantic checks use LLM (thorough, but cost-aware)
 *   <li>Sampling strategy for large graphs (validate subset, extrapolate)
 * </ul>
 */
public class GraphValidationService {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(GraphValidationService.class);

  private final HandbookGraphService graphService;
  private final LlmClient llmClient;
  private final boolean enableStructuralValidation;
  private final boolean enableSemanticValidation;
  private final boolean semanticRequireStructuralPass;
  private final int maxSamplesPerValidation;

  public GraphValidationService(OneMcp oneMcp, HandbookGraphService graphService) {
    this.graphService = graphService;
    this.llmClient = oneMcp.llmClient();

    this.enableStructuralValidation =
        oneMcp.configuration().getBoolean("indexing.graph.validation.structural.enabled", true);
    this.enableSemanticValidation =
        oneMcp.configuration().getBoolean("indexing.graph.validation.semantic.enabled", true);
    this.semanticRequireStructuralPass =
        oneMcp.configuration()
            .getBoolean("indexing.graph.validation.semantic.requireStructuralPass", true);
    this.maxSamplesPerValidation =
        oneMcp.configuration().getInt("indexing.graph.validation.semantic.maxSamples", 10);
  }

  /**
   * Comprehensive validation of the knowledge graph.
   *
   * @return validation report with issues and recommendations
   */
  public ValidationReport validate() {
    return validate(false);
  }

  /**
   * Comprehensive validation of the knowledge graph.
   *
   * @param force if true, run validation (used by runAfterIndexing)
   * @return validation report with issues and recommendations
   */
  public ValidationReport validate(boolean force) {
    // Validation always runs when called (no global enabled flag needed)
    // runAfterIndexing controls automatic execution, manual calls always work

    log.info("Starting knowledge graph validation...");
    ValidationReport report = new ValidationReport();

    // Get initial statistics
    List<Map<String, Object>> allNodes = graphService.retrieveByContext(List.of());
    log.debug("Retrieved {} total nodes from graph", allNodes.size());
    List<Map<String, Object>> allDocuments =
        allNodes.stream()
            .filter(n -> "DOCUMENT".equals(n.get("nodeType")))
            .collect(Collectors.toList());
    List<Map<String, Object>> allChunks =
        allNodes.stream()
            .filter(n -> "DOCS_CHUNK".equals(n.get("nodeType")))
            .collect(Collectors.toList());
    log.debug("Found {} document nodes and {} chunk nodes", allDocuments.size(), allChunks.size());

    // Phase 1: Structural validation (deterministic, fast)
    if (enableStructuralValidation) {
      validateStructure(report);
      report.setStructuralValidationRun(true);
    }

    // Phase 2: Semantic validation (LLM-based, thorough but sampled)
    boolean shouldRunSemantic =
        enableSemanticValidation
            && (!semanticRequireStructuralPass || !report.hasErrors());
    int documentsValidatedSuccessfully = 0;
    int documentsValidatedFailed = 0;
    int chunksValidatedSuccessfully = 0;
    int chunksValidatedFailed = 0;
    if (shouldRunSemantic) {
      int[] stats = validateSemantics(report);
      documentsValidatedSuccessfully = stats[0];
      documentsValidatedFailed = stats[1];
      chunksValidatedSuccessfully = stats[2];
      chunksValidatedFailed = stats[3];
      report.setSemanticValidationRun(true);
    } else if (enableSemanticValidation && semanticRequireStructuralPass && report.hasErrors()) {
      log.info(
          "Skipping semantic validation due to structural errors (requireStructuralPass=true)");
    }

    // Set statistics in report
    report.setStatistics(
        allDocuments.size(), allChunks.size(), 
        documentsValidatedSuccessfully, documentsValidatedFailed,
        chunksValidatedSuccessfully, chunksValidatedFailed);

    log.info(
        "Validation complete: {} errors, {} warnings",
        report.getErrors().size(),
        report.getWarnings().size());
    return report;
  }

  /**
   * Structural validation using deterministic checks (no LLM).
   */
  private void validateStructure(ValidationReport report) {
    log.debug("Phase 1: Structural validation");

    List<Map<String, Object>> allNodes = graphService.retrieveByContext(List.of());

    // Group nodes by type
    List<Map<String, Object>> documents =
        allNodes.stream()
            .filter(n -> "DOCUMENT".equals(n.get("nodeType")))
            .collect(Collectors.toList());
    List<Map<String, Object>> chunks =
        allNodes.stream()
            .filter(n -> "DOCS_CHUNK".equals(n.get("nodeType")))
            .collect(Collectors.toList());

    // Check 1: Document nodes exist
    if (documents.isEmpty()) {
      report.addError("No DOCUMENT nodes found in graph");
    } else {
      log.debug("Found {} document nodes", documents.size());
    }

    // Check 2: All chunks have parentDocumentKey
    List<String> orphanedChunks = new ArrayList<>();
    for (Map<String, Object> chunk : chunks) {
      String parentKey = (String) chunk.get("parentDocumentKey");
      if (parentKey == null || parentKey.isBlank()) {
        orphanedChunks.add((String) chunk.get("key"));
      }
    }
    if (!orphanedChunks.isEmpty()) {
      report.addError(
          String.format(
              "Found %d chunks without parentDocumentKey: %s",
              orphanedChunks.size(), orphanedChunks.subList(0, Math.min(5, orphanedChunks.size()))));
    }

    // Check 3: All parentDocumentKey values reference existing documents
    Set<String> documentKeys =
        documents.stream().map(d -> (String) d.get("key")).collect(Collectors.toSet());
    List<String> invalidParents = new ArrayList<>();
    for (Map<String, Object> chunk : chunks) {
      String parentKey = (String) chunk.get("parentDocumentKey");
      if (parentKey != null && !documentKeys.contains(parentKey)) {
        invalidParents.add((String) chunk.get("key") + " -> " + parentKey);
      }
    }
    if (!invalidParents.isEmpty()) {
      report.addError(
          String.format(
              "Found %d chunks with invalid parentDocumentKey: %s",
              invalidParents.size(), invalidParents.subList(0, Math.min(5, invalidParents.size()))));
    }

    // Check 4: Document entities are union of chunk entities
    for (Map<String, Object> doc : documents) {
      String docKey = (String) doc.get("key");
      @SuppressWarnings("unchecked")
      List<String> docEntities =
          (List<String>) doc.getOrDefault("entities", Collections.emptyList());

      // Find all chunks for this document
      List<String> chunkEntities =
          chunks.stream()
              .filter(c -> docKey.equals(c.get("parentDocumentKey")))
              .flatMap(
                  c -> {
                    @SuppressWarnings("unchecked")
                    List<String> ents =
                        (List<String>) c.getOrDefault("entities", Collections.emptyList());
                    return ents.stream();
                  })
              .distinct()
              .collect(Collectors.toList());

      // Check if document entities are subset of chunk entities union
      Set<String> docEntitySet = new HashSet<>(docEntities);
      Set<String> chunkEntitySet = new HashSet<>(chunkEntities);
      if (!chunkEntitySet.containsAll(docEntitySet)) {
        Set<String> missing = new HashSet<>(docEntitySet);
        missing.removeAll(chunkEntitySet);
        report.addWarning(
            String.format(
                "Document %s has entities not found in chunks: %s", docKey, missing));
      }
    }

    // Check 5: Chunk-to-document ratio (sanity check)
    if (!documents.isEmpty()) {
      double avgChunksPerDoc = (double) chunks.size() / documents.size();
      if (avgChunksPerDoc < 1.0) {
        report.addWarning(
            String.format(
                "Average chunks per document is %.2f (expected >= 1.0)", avgChunksPerDoc));
      }
    }
  }

  /**
   * Semantic validation using LLM (sampled for cost efficiency).
   * @return array with [documentsValidatedSuccessfully, documentsValidatedFailed, chunksValidatedSuccessfully, chunksValidatedFailed]
   */
  private int[] validateSemantics(ValidationReport report) {
    log.debug("Phase 2: Semantic validation (LLM-based, sampled)");

    List<Map<String, Object>> allNodes = graphService.retrieveByContext(List.of());
    
    // Get valid entities from the graph nodes (entities that were extracted and stored)
    List<String> validEntities = getValidEntities(allNodes);
    log.debug("Found {} valid entities in graph: {}", validEntities.size(), validEntities);
    
    List<Map<String, Object>> documents =
        allNodes.stream()
            .filter(n -> "DOCUMENT".equals(n.get("nodeType")))
            .collect(Collectors.toList());
    List<Map<String, Object>> chunks =
        allNodes.stream()
            .filter(n -> "DOCS_CHUNK".equals(n.get("nodeType")))
            .collect(Collectors.toList());

    // Sample documents for validation (to control cost)
    Collections.shuffle(documents);
    List<Map<String, Object>> samples =
        documents.stream().limit(maxSamplesPerValidation).collect(Collectors.toList());

    log.info("Validating {} document(s) out of {} (sampling for cost efficiency)", samples.size(), documents.size());

    int documentsValidatedSuccessfully = 0;
    int documentsValidatedFailed = 0;
    int chunksValidatedSuccessfully = 0;
    int chunksValidatedFailed = 0;

    int docIndex = 0;
    for (Map<String, Object> doc : samples) {
      docIndex++;
      String docKey = (String) doc.get("key");
      log.info("Validating document {}/{}: {}", docIndex, samples.size(), docKey);

      // Get all chunks for this document
      List<Map<String, Object>> docChunks =
          chunks.stream()
              .filter(c -> docKey.equals(c.get("parentDocumentKey")))
              .collect(Collectors.toList());

      if (docChunks.isEmpty()) {
        report.addWarning(String.format("Document %s has no chunks", docKey));
        log.info("  Skipping document {}: no chunks", docKey);
        continue;
      }

      log.debug("  Document has {} chunks", docChunks.size());

      // Track if this document's validation succeeded or failed
      boolean documentValidationSucceeded = true;
      int initialWarningCount = report.getWarnings().size();

      // Validate document-chunk relationship
      log.info("  Step 1/3: Validating document-chunk relationship (this may take a moment for LLM call)...");
      try {
        validateDocumentChunkRelationship(doc, docChunks, report, validEntities);
        log.info("  Step 1/3: ✓ Completed");
      } catch (Exception e) {
        documentValidationSucceeded = false;
        log.warn("  Step 1/3: ✗ Failed: {}", e.getMessage());
      }

      // Validate entity extraction accuracy
      log.info("  Step 2/3: Validating entity extraction (this may take a moment for LLM calls)...");
      try {
        validateEntityExtraction(doc, docChunks, report, validEntities);
        log.info("  Step 2/3: ✓ Completed");
      } catch (Exception e) {
        documentValidationSucceeded = false;
        log.warn("  Step 2/3: ✗ Failed: {}", e.getMessage());
      }

      // Validate chunk content coherence
      log.info("  Step 3/3: Validating chunk coherence (this may take a moment for LLM call)...");
      try {
        validateChunkCoherence(docChunks, report);
        log.info("  Step 3/3: ✓ Completed");
      } catch (Exception e) {
        documentValidationSucceeded = false;
        log.warn("  Step 3/3: ✗ Failed: {}", e.getMessage());
      }
      
      log.info("  ✓ Completed validation for document {}", docKey);

      // Check if new warnings were added indicating LLM validation failures
      // (warnings like "LLM validation failed" indicate actual failures, not just issues found)
      int finalWarningCount = report.getWarnings().size();
      boolean hasLLMFailure = false;
      if (finalWarningCount > initialWarningCount) {
        hasLLMFailure = report.getWarnings().stream()
            .skip(initialWarningCount)
            .anyMatch(w -> w.contains("LLM validation failed"));
      }

      if (hasLLMFailure || !documentValidationSucceeded) {
        documentsValidatedFailed++;
        chunksValidatedFailed += docChunks.size();
        report.setSemanticValidationPartialFailure(true);
      } else {
        documentsValidatedSuccessfully++;
        chunksValidatedSuccessfully += docChunks.size();
      }
    }
    
    return new int[] {documentsValidatedSuccessfully, documentsValidatedFailed, 
                      chunksValidatedSuccessfully, chunksValidatedFailed};
  }

  /**
   * Get the list of valid entities from the graph nodes (entities that were extracted and stored).
   * This uses the same entities that EntityExtractor uses, collected from all indexed nodes.
   */
  private List<String> getValidEntities(List<Map<String, Object>> allNodes) {
    Set<String> validEntities = new HashSet<>();
    
    // Collect entities from all node types in the graph
    for (Map<String, Object> node : allNodes) {
      @SuppressWarnings("unchecked")
      List<String> nodeEntities = (List<String>) node.getOrDefault("entities", Collections.emptyList());
      if (nodeEntities != null) {
        validEntities.addAll(nodeEntities);
      }
    }
    
    return validEntities.stream()
        .filter(Objects::nonNull)
        .filter(s -> !s.isEmpty())
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * Validate that document entities accurately represent chunk content.
   */
  private void validateDocumentChunkRelationship(
      Map<String, Object> doc, List<Map<String, Object>> chunks, ValidationReport report, List<String> validEntities) {
    String docKey = (String) doc.get("key");
    @SuppressWarnings("unchecked")
    List<String> docEntities =
        (List<String>) doc.getOrDefault("entities", Collections.emptyList());

    // Collect all chunk content
    String allChunkContent =
        chunks.stream()
            .map(c -> (String) c.getOrDefault("content", ""))
            .collect(Collectors.joining("\n\n---\n\n"));

    // Build prompt for LLM validation
    String prompt =
        String.format(
            """
            You are validating a knowledge graph structure. A document node has been created with the following entities: %s
            
            IMPORTANT: Only consider entities from this VALID ENTITIES LIST: %s
            
            The document contains %d chunks with the following combined content:
            
            %s
            
            Please analyze:
            1. Are the document entities (%s) accurately represented in the chunk content?
            2. Are there any VALID entities (from the list above) mentioned in the content that are missing from the document entities?
            3. Is the parent-child relationship between document and chunks semantically correct?
            
            Respond in JSON format:
            {
              "entitiesAccurate": true/false,
              "missingEntities": ["entity1", "entity2"],  // ONLY entities from the valid list
              "relationshipValid": true/false,
              "issues": ["issue1", "issue2"],
              "confidence": 0.0-1.0
            }
            """,
            docEntities,
            validEntities,
            chunks.size(),
            allChunkContent.substring(0, Math.min(2000, allChunkContent.length())),
            docEntities);

    try {
      log.info("    Calling LLM for document-chunk relationship validation (prompt: {} chars)...", prompt.length());
      long startTime = System.currentTimeMillis();
      String response =
          llmClient.generate(
              prompt, Collections.emptyList(), false, null);
      long duration = System.currentTimeMillis() - startTime;
      log.info("    LLM response received (took {}ms, {} chars)", 
          duration, response != null ? response.length() : 0);

      // Extract JSON from response
      String jsonStr = StringUtility.extractSnippet(response, "json");
      if (jsonStr == null || jsonStr.isBlank()) {
        jsonStr = response; // Fallback to full response if no JSON snippet found
      }

      // Parse JSON response (simplified - check for issues)
      if (jsonStr.contains("\"entitiesAccurate\": false")
          || jsonStr.contains("\"relationshipValid\": false")) {
        report.addWarning(
            String.format(
                "Document %s: LLM validation found semantic issues. Response: %s",
                docKey, jsonStr));
      }
    } catch (Exception e) {
      log.warn(
          "LLM validation failed for document {}: {} ({})",
          docKey, e.getMessage(), e.getClass().getSimpleName(), e);
      log.warn(
          "  Validation context: prompt length={} chars, chunks={}, entities={}",
          prompt.length(), chunks.size(), docEntities);
      report.addWarning(
          String.format("LLM validation failed for document %s: %s (%s)", 
              docKey, e.getMessage(), e.getClass().getSimpleName()));
    }
  }

  /**
   * Validate that extracted entities match chunk content.
   */
  private void validateEntityExtraction(
      Map<String, Object> doc, List<Map<String, Object>> chunks, ValidationReport report, List<String> validEntities) {
    log.info("    Processing {} chunks for entity extraction validation...", chunks.size());
    int chunkIndex = 0;
    for (Map<String, Object> chunk : chunks) {
      chunkIndex++;
      String chunkKey = (String) chunk.get("key");
      String content = (String) chunk.getOrDefault("content", "");
      @SuppressWarnings("unchecked")
      List<String> entities =
          (List<String>) chunk.getOrDefault("entities", Collections.emptyList());
      
      log.info("    Chunk {}/{}: {} (entities: {})", 
          chunkIndex, chunks.size(), chunkKey, 
          entities.isEmpty() ? "none" : String.join(", ", entities));
      
      // Track timing for this chunk validation
      long chunkStartTime = System.currentTimeMillis();
      
      if (entities.isEmpty()) {
        // Orphaned chunk - validate it should be orphaned
        String prompt =
            String.format(
                """
                A chunk from a knowledge graph has no extracted entities. The chunk content is:
                
                %s
                
                IMPORTANT: Only consider entities from this VALID ENTITIES LIST: %s
                
                Please analyze:
                1. Does this chunk contain any VALID entities (from the list above)?
                2. Should this chunk have entities extracted, or is it correctly orphaned (e.g., generic documentation)?
                
                Respond in JSON:
                {
                  "shouldHaveEntities": true/false,
                  "suggestedEntities": ["entity1", "entity2"],  // ONLY entities from the valid list
                  "reason": "explanation"
                }
                """,
                content.substring(0, Math.min(1000, content.length())),
                validEntities);

        try {
          log.info("      Calling LLM to validate if orphaned chunk should have entities...");
          long startTime = System.currentTimeMillis();
          String response = llmClient.generate(prompt, Collections.emptyList(), false, null);
          long duration = System.currentTimeMillis() - startTime;
          log.info("      LLM response received (took {}ms, {} chars)", 
              duration, response != null ? response.length() : 0);
          String jsonStr = StringUtility.extractSnippet(response, "json");
          if (jsonStr == null || jsonStr.isBlank()) {
            jsonStr = response;
          }
          if (jsonStr.contains("\"shouldHaveEntities\": true")) {
            report.addWarning(
                String.format(
                    "Chunk %s appears to be incorrectly orphaned (no entities but content suggests entities exist)",
                    chunkKey));
          }
        } catch (Exception e) {
          log.warn("      ✗ LLM call failed for chunk {}: {} ({})", 
              chunkKey, e.getMessage(), e.getClass().getSimpleName());
        }
      } else {
        // Validate entities match content
        String prompt =
            String.format(
                """
                A chunk has been assigned these entities: %s
                
                IMPORTANT: Only consider entities from this VALID ENTITIES LIST: %s
                
                Chunk content:
                %s
                
                Please verify:
                1. Are all assigned entities actually mentioned or relevant to the content?
                2. Are there any VALID entities (from the list above) in the content that are missing from the assignment?
                
                Respond in JSON:
                {
                  "entitiesMatch": true/false,
                  "missingEntities": ["entity1"],  // ONLY entities from the valid list
                  "irrelevantEntities": ["entity2"],  // Entities assigned but not relevant
                  "confidence": 0.0-1.0
                }
                """,
                entities,
                validEntities,
                content.substring(0, Math.min(1000, content.length())));

        try {
          log.info("      Calling LLM to validate entity extraction accuracy...");
          long startTime = System.currentTimeMillis();
          String response = llmClient.generate(prompt, Collections.emptyList(), false, null);
          long duration = System.currentTimeMillis() - startTime;
          log.info("      LLM response received (took {}ms, {} chars)", 
              duration, response != null ? response.length() : 0);
          String jsonStr = StringUtility.extractSnippet(response, "json");
          if (jsonStr == null || jsonStr.isBlank()) {
            jsonStr = response;
          }
          if (jsonStr.contains("\"entitiesMatch\": false")) {
            report.addWarning(
                String.format(
                    "Chunk %s: Entity extraction may be inaccurate. Response: %s",
                    chunkKey, jsonStr));
          }
        } catch (Exception e) {
          log.warn("      ✗ LLM call failed for chunk {}: {} ({})", 
              chunkKey, e.getMessage(), e.getClass().getSimpleName());
        }
      }
      
      // Record timing for this chunk
      long chunkDuration = System.currentTimeMillis() - chunkStartTime;
      report.addChunkValidationTime(chunkKey, chunkDuration);
      log.info("    ✓ Completed chunk {}/{} (took {}ms)", chunkIndex, chunks.size(), chunkDuration);
    }
    log.info("    ✓ Completed entity extraction validation for all {} chunks", chunks.size());
  }

  /**
   * Validate that chunks maintain semantic coherence and don't lose important information.
   */
  private void validateChunkCoherence(
      List<Map<String, Object>> chunks, ValidationReport report) {
    if (chunks.size() < 2) {
      return; // Need multiple chunks to check coherence
    }

    // Sample a few chunks to check coherence
    Collections.shuffle(chunks);
    List<Map<String, Object>> samples =
        chunks.stream().limit(Math.min(3, chunks.size())).collect(Collectors.toList());

    String allContent =
        samples.stream()
            .map(c -> (String) c.getOrDefault("content", ""))
            .collect(Collectors.joining("\n\n---\n\n"));

    String prompt =
        String.format(
            """
            These are consecutive chunks from a document that was split:
            
            %s
            
            Please analyze:
            1. Do the chunks maintain semantic coherence (do they make sense as separate units)?
            2. Is important information preserved across chunk boundaries?
            3. Are there any critical concepts that seem fragmented or lost?
            
            Respond in JSON:
            {
              "coherent": true/false,
              "informationPreserved": true/false,
              "fragmentationIssues": ["issue1"],
              "recommendations": ["rec1"]
            }
            """,
            allContent.substring(0, Math.min(2000, allContent.length())));

    try {
      log.debug("    Calling LLM for chunk coherence validation");
      String response = llmClient.generate(prompt, Collections.emptyList(), false, null);
      log.debug("    LLM response received for chunk coherence");
      String jsonStr = StringUtility.extractSnippet(response, "json");
      if (jsonStr == null || jsonStr.isBlank()) {
        jsonStr = response;
      }
      if (jsonStr.contains("\"coherent\": false")
          || jsonStr.contains("\"informationPreserved\": false")) {
        report.addWarning(
            "Chunk coherence validation found potential issues with chunk boundaries or information preservation");
      }
    } catch (Exception e) {
      log.debug("Chunk coherence validation skipped: {} ({})", 
          e.getMessage(), e.getClass().getSimpleName(), e);
    }
  }

  /**
   * Validation report containing errors, warnings, and recommendations.
   */
  public static class ValidationReport {
    private final List<String> errors = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final List<String> recommendations = new ArrayList<>();
    
    // Statistics
    private int totalDocuments = 0;
    private int totalChunks = 0;
    private int documentsValidatedSuccessfully = 0;
    private int documentsValidatedFailed = 0;
    private int chunksValidatedSuccessfully = 0;
    private int chunksValidatedFailed = 0;
    private boolean structuralValidationRun = false;
    private boolean semanticValidationRun = false;
    private boolean semanticValidationPartialFailure = false;
    
    // Timing information
    private final Map<String, Long> chunkValidationTimes = new LinkedHashMap<>(); // chunkKey -> milliseconds
    private long totalChunkValidationTimeMs = 0;

    public void addError(String error) {
      errors.add(error);
    }

    public void addWarning(String warning) {
      warnings.add(warning);
    }

    public void addRecommendation(String recommendation) {
      recommendations.add(recommendation);
    }

    public List<String> getErrors() {
      return Collections.unmodifiableList(errors);
    }

    public List<String> getWarnings() {
      return Collections.unmodifiableList(warnings);
    }

    public List<String> getRecommendations() {
      return Collections.unmodifiableList(recommendations);
    }

    public boolean hasErrors() {
      return !errors.isEmpty();
    }

    public boolean isValid() {
      return errors.isEmpty() && warnings.isEmpty();
    }
    
    public void setStatistics(int totalDocuments, int totalChunks, 
        int documentsValidatedSuccessfully, int documentsValidatedFailed,
        int chunksValidatedSuccessfully, int chunksValidatedFailed) {
      this.totalDocuments = totalDocuments;
      this.totalChunks = totalChunks;
      this.documentsValidatedSuccessfully = documentsValidatedSuccessfully;
      this.documentsValidatedFailed = documentsValidatedFailed;
      this.chunksValidatedSuccessfully = chunksValidatedSuccessfully;
      this.chunksValidatedFailed = chunksValidatedFailed;
    }
    
    public void setSemanticValidationPartialFailure(boolean partialFailure) {
      this.semanticValidationPartialFailure = partialFailure;
    }
    
    public void setStructuralValidationRun(boolean run) {
      this.structuralValidationRun = run;
    }
    
    public void setSemanticValidationRun(boolean run) {
      this.semanticValidationRun = run;
    }
    
    public void addChunkValidationTime(String chunkKey, long timeMs) {
      chunkValidationTimes.put(chunkKey, timeMs);
      totalChunkValidationTimeMs += timeMs;
    }
    
    public Map<String, Long> getChunkValidationTimes() {
      return Collections.unmodifiableMap(chunkValidationTimes);
    }
    
    public long getTotalChunkValidationTimeMs() {
      return totalChunkValidationTimeMs;
    }
    
    private String formatDuration(long milliseconds) {
      if (milliseconds < 1000) {
        return milliseconds + "ms";
      } else if (milliseconds < 60000) {
        return String.format("%.2fs", milliseconds / 1000.0);
      } else {
        long seconds = milliseconds / 1000;
        long minutes = seconds / 60;
        seconds = seconds % 60;
        return String.format("%dm %ds", minutes, seconds);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("=== Graph Validation Report ===\n");
      
      // Statistics section
      sb.append("Statistics:\n");
      sb.append("  Total documents in graph: ").append(totalDocuments).append("\n");
      sb.append("  Total chunks in graph: ").append(totalChunks).append("\n");
      if (semanticValidationRun) {
        int totalDocsValidated = documentsValidatedSuccessfully + documentsValidatedFailed;
        int totalChunksValidated = chunksValidatedSuccessfully + chunksValidatedFailed;
        sb.append("  Documents validated (semantic): ").append(totalDocsValidated);
        if (documentsValidatedFailed > 0) {
          sb.append(" (✓ ").append(documentsValidatedSuccessfully)
            .append(" successful, ✗ ").append(documentsValidatedFailed).append(" failed)");
        } else if (totalDocsValidated > 0) {
          sb.append(" (all successful)");
        }
        sb.append("\n");
        sb.append("  Chunks validated (semantic): ").append(totalChunksValidated);
        if (chunksValidatedFailed > 0) {
          sb.append(" (✓ ").append(chunksValidatedSuccessfully)
            .append(" successful, ✗ ").append(chunksValidatedFailed).append(" failed)");
        } else if (totalChunksValidated > 0) {
          sb.append(" (all successful)");
        }
        if (totalChunkValidationTimeMs > 0) {
          sb.append(" (total time: ").append(formatDuration(totalChunkValidationTimeMs)).append(")");
        }
        sb.append("\n");
      }
      sb.append("  Structural validation: ").append(structuralValidationRun ? "✓ Run" : "✗ Skipped").append("\n");
      if (semanticValidationRun) {
        if (semanticValidationPartialFailure) {
          sb.append("  Semantic validation: ⚠ Partially failed (some LLM calls failed)\n");
        } else if (documentsValidatedSuccessfully > 0 || documentsValidatedFailed > 0) {
          sb.append("  Semantic validation: ✓ Run (all successful)\n");
        } else {
          sb.append("  Semantic validation: ✓ Run\n");
        }
      } else {
        sb.append("  Semantic validation: ✗ Skipped\n");
      }
      sb.append("\n");
      
      // Results section
      if (errors.isEmpty() && warnings.isEmpty()) {
        sb.append("Result: ✅ All validations passed!\n");
      } else {
        if (!errors.isEmpty()) {
          sb.append("Result: ❌ Validation found errors\n");
          sb.append("Errors (").append(errors.size()).append("):\n");
          for (String error : errors) {
            sb.append("  - ").append(error).append("\n");
          }
          sb.append("\n");
        }
        if (!warnings.isEmpty()) {
          sb.append("Warnings (").append(warnings.size()).append("):\n");
          for (String warning : warnings) {
            sb.append("  - ").append(warning).append("\n");
          }
          sb.append("\n");
        }
      }
      
      // Chunk validation timing details
      if (!chunkValidationTimes.isEmpty()) {
        sb.append("Chunk Validation Timing:\n");
        for (Map.Entry<String, Long> entry : chunkValidationTimes.entrySet()) {
          sb.append("  - ").append(entry.getKey())
            .append(": ").append(formatDuration(entry.getValue())).append("\n");
        }
        sb.append("\n");
      }
      
      if (!recommendations.isEmpty()) {
        sb.append("Recommendations:\n");
        for (String rec : recommendations) {
          sb.append("  - ").append(rec).append("\n");
        }
      }
      
      return sb.toString();
    }
  }
}

