package com.gentoro.onemcp.utility;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CollectionUtility {

  public static <K, V> Map<K, V> mapOf(Class<K> keyType, Class<V> valueType, Object... values) {
    if (values.length % 2 != 0) {
      throw new IllegalArgumentException("Number of arguments must be even");
    }

    Map<K, V> map = new HashMap<>();

    for (int i = 0; i < values.length; i += 2) {
      if (!keyType.isInstance(values[i])) {
        throw new IllegalArgumentException(
            "Key at position " + i + " is not of type " + keyType.getName());
      }
      if (!valueType.isInstance(values[i + 1])) {
        throw new IllegalArgumentException(
            "Value at position " + (i + 1) + " is not of type " + valueType.getName());
      }

      map.put(keyType.cast(values[i]), valueType.cast(values[i + 1]));
    }

    return map;
  }

  public static <K, V> Map<K, V> mergeMaps(Map<K, V>... maps) {
    Map<K, V> result = new HashMap<>();
    if (Objects.nonNull(maps)) {
      for (Map<K, V> map : maps) {
        if (Objects.nonNull(map)) {
          result.putAll(map);
        }
      }
    }
    return result;
  }
}
