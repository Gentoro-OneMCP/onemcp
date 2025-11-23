package com.gentoro.onemcp.indexing;

import com.gentoro.onemcp.indexing.graph.GraphEdge;
import com.gentoro.onemcp.indexing.graph.nodes.*;
import java.util.List;

/**
 * Common types used by graph indexing components.
 *
 * <p>This class contains shared data structures used across the indexing package, particularly for
 * communication between GraphIndexingService and GraphIndexingLogger.
 */
public final class GraphIndexingTypes {

  private GraphIndexingTypes() {}

  /** Record to hold extraction results from LLM parsing. */
  public record GraphExtractionResult(
      List<EntityNode> entities,
      List<FieldNode> fields,
      List<OperationNode> operations,
      List<ExampleNode> examples,
      List<DocumentationNode> documentations,
      List<GraphEdge> relationships) {}
}
