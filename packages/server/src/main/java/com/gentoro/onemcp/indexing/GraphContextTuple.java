package com.gentoro.onemcp.indexing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Tuple used as retrieval input: an Entity and zero-or-more Operations. */
public class GraphContextTuple {
  private final String entity; // required
  private final List<String> operations; // optional

  public GraphContextTuple(String entity, List<String> operations) {
    this.entity = Objects.requireNonNull(entity, "entity");
    this.operations =
        new ArrayList<>(Objects.requireNonNullElse(operations, Collections.emptyList()));
  }

  public String getEntity() {
    return entity;
  }

  public List<String> getOperations() {
    return Collections.unmodifiableList(operations);
  }
}
