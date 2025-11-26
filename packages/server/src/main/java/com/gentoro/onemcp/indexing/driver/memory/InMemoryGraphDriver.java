package com.gentoro.onemcp.indexing.driver.memory;

import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Simple in-memory GraphDriver implementation for testing and default usage. */
public class InMemoryGraphDriver implements GraphDriver {
  private final String handbookName;
  private volatile boolean initialized = false;
  private final Map<String, Map<String, Object>> store = new ConcurrentHashMap<>();

  public InMemoryGraphDriver(String handbookName) {
    this.handbookName = handbookName != null ? handbookName : "default";
  }

  @Override
  public void initialize() {
    initialized = true;
  }

  @Override
  public boolean isInitialized() {
    return initialized;
  }

  @Override
  public void clearAll() {
    store.clear();
  }

  @Override
  public void upsertNodes(List<GraphNodeRecord> nodes) {
    for (GraphNodeRecord n : nodes) {
      store.put(n.getKey(), n.toMap());
    }
  }

  @Override
  public List<Map<String, Object>> queryByContext(List<GraphContextTuple> contextTuples) {
    if (contextTuples == null || contextTuples.isEmpty()) {
      return new ArrayList<>(store.values());
    }
    // Build lookup maps
    Set<String> entities = new HashSet<>();
    Map<String, Set<String>> opsByEntity = new HashMap<>();
    for (GraphContextTuple t : contextTuples) {
      entities.add(t.getEntity());
      opsByEntity.computeIfAbsent(t.getEntity(), k -> new HashSet<>()).addAll(t.getOperations());
    }

    return store.values().stream()
        .filter(
            map -> {
              @SuppressWarnings("unchecked")
              List<String> nodeEntities =
                  (List<String>) map.getOrDefault("entities", Collections.emptyList());
              @SuppressWarnings("unchecked")
              List<String> nodeOps =
                  (List<String>) map.getOrDefault("operations", Collections.emptyList());

              // Must match at least one entity
              Optional<String> entityMatch =
                  nodeEntities.stream().filter(entities::contains).findFirst();
              if (entityMatch.isEmpty()) return false;
              String matchedEntity = entityMatch.get();

              // If node has no operations, include (entity-only link)
              if (nodeOps == null || nodeOps.isEmpty()) return true;

              // Otherwise, must intersect with requested operations for the matched entity
              Set<String> requested =
                  opsByEntity.getOrDefault(matchedEntity, Collections.emptySet());
              if (requested.isEmpty()) return true; // if context has only entity, include
              for (String op : nodeOps) {
                if (requested.contains(op)) return true;
              }
              return false;
            })
        .map(map -> new LinkedHashMap<>(map))
        .collect(Collectors.toList());
  }

  @Override
  public void deleteNodesByKeys(List<String> keys) {
    if (keys == null) return;
    for (String k : keys) store.remove(k);
  }

  @Override
  public String getDriverName() {
    return "in-memory";
  }

  @Override
  public String getHandbookName() {
    return handbookName;
  }

  @Override
  public void shutdown() {
    initialized = false;
    store.clear();
  }
}
