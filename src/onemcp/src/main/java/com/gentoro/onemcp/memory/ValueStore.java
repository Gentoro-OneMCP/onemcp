package com.gentoro.onemcp.memory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ValueStore {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ValueStore.class);
  private final Map<String, Value> memory = new ConcurrentHashMap<>();

  public void put(Value entry) {
    memory.put(entry.getIdentifier(), entry);
    log.trace("ValueStore: put {}", entry);
  }

  public Value get(String id) {
    try {
      return memory.get(id);
    } finally {
      log.trace("ValueStore: get {}", id);
    }
  }

  public List<Value> getAll(String... ids) {
    log.trace("ValueStore: getAll {}", String.join(",", ids));
    List<Value> result = new ArrayList<>();
    Arrays.stream(ids)
        .forEach(
            slug -> {
              if (memory.containsKey(slug)) {
                result.add(memory.get(slug));
              } else if (memory.get(slug) == null && slug.endsWith("*")) {
                result.addAll(
                    memory.values().stream()
                        .filter(
                            e -> e.getIdentifier().startsWith(slug.substring(0, slug.length() - 1)))
                        .toList());
              }
            });
    return result;
  }

  public Map<String, Value> list() {
    try {
      return Collections.unmodifiableMap(memory);
    } finally {
      log.trace("ValueStore: list");
    }
  }

  public void clear() {
    memory.clear();
    log.trace("ValueStore: clear");
  }
}
