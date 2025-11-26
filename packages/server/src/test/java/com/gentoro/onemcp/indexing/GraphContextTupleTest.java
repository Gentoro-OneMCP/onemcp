package com.gentoro.onemcp.indexing;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GraphContextTupleTest {

  @Test
  @DisplayName("Entity is required and operations list is unmodifiable copy")
  void immutabilityAndNulls() {
    List<String> ops = new ArrayList<>();
    ops.add("Retrieve");

    GraphContextTuple t = new GraphContextTuple("Order", ops);
    // mutate original list should not affect tuple
    ops.add("Create");

    assertEquals("Order", t.getEntity());
    assertEquals(List.of("Retrieve"), t.getOperations());

    // returned list must be unmodifiable
    assertThrows(UnsupportedOperationException.class, () -> t.getOperations().add("X"));

    GraphContextTuple t2 = new GraphContextTuple("User", null);
    assertNotNull(t2.getOperations());
    assertTrue(t2.getOperations().isEmpty());
  }
}
