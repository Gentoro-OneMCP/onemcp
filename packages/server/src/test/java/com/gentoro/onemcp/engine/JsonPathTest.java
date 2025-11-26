package com.gentoro.onemcp.engine;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;

public class JsonPathTest {

  private static final String SAMPLE =
      """
    {
      "store": {
        "book": [
          { "category": "reference", "author": "Nigel Rees", "price": 8.95 },
          { "category": "fiction", "author": "Evelyn Waugh", "price": 12.99 },
          { "category": "fiction", "author": "Herman Melville", "price": 8.99, "isbn": "0-553-21311-3" },
          { "category": "fiction", "author": "J. R. R. Tolkien", "price": 22.99 }
        ],
        "bicycle": {
          "color": "red", "price": 19.95
        }
      },
      "expensive": 10,
      "nullField": null
    }
    """;

  @Test
  void testRoot() throws Exception {
    JsonNode node = JsonPath.read(SAMPLE, "$");
    assertTrue(node.isObject());
    assertNotNull(node.get("store"));
  }

  @Test
  void testFieldSelection() throws Exception {
    JsonNode node = JsonPath.read(SAMPLE, "$.store.book");
    assertTrue(node.isArray());
    assertEquals(4, node.size());
  }

  @Test
  void testWildcard() throws Exception {
    JsonNode node = JsonPath.read(SAMPLE, "$.store.*");
    assertTrue(node.isArray());
    assertEquals(2, node.size());
  }

  @Test
  void testRecursiveDescent() throws Exception {
    JsonNode authors = JsonPath.read(SAMPLE, "$..author");
    assertTrue(authors.isArray());
    assertEquals(4, authors.size());
  }

  @Test
  void testArrayIndex() throws Exception {
    JsonNode item = JsonPath.read(SAMPLE, "$.store.book[2]");
    assertEquals("Herman Melville", item.get("author").asText());
  }

  @Test
  void testArrayIndexOutOfBounds() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[10]");
    assertTrue(result.isNull());
  }

  @Test
  void testArrayMultiIndex() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[0,2]");
    assertEquals(2, result.size());
  }

  @Test
  void testArraySlice() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[1:3]");
    assertEquals("Evelyn Waugh", result.get(0).get("author").asText());
  }

  @Test
  void testArraySliceStep() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[0:4:2]");
    assertEquals(2, result.size());
  }

  @Test
  void testArrayWildcard() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[*]");
    assertEquals(4, result.size());
  }

  @Test
  void testFilterNumericComparison() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[?(@.price > 10)]");
    assertEquals(2, result.size());
  }

  @Test
  void testFilterFieldExists() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.store.book[?(@.isbn)]");
    assertTrue(result.isArray());
    assertEquals(1, result.size());
    assertEquals("Herman Melville", result.get(0).get("author").asText());
  }

  @Test
  void testNullFieldAccess() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.nullField");
    assertTrue(result.isNull());
  }

  @Test
  void testInvalidFieldAccess() throws Exception {
    JsonNode result = JsonPath.read(SAMPLE, "$.expensive.something");
    assertTrue(result.isNull());
  }

  @Test
  void testDeepWildcard() throws Exception {
    List<JsonNode> nodes = JsonPath.evaluate(JsonPath.read(SAMPLE, "$"), "$..*");
    assertFalse(nodes.isEmpty());
  }

  // ---- New function tests: size(), limit(n), sort(expr) ----

  @Test
  void testSizeOnArray() throws Exception {
    JsonNode size = JsonPath.read(SAMPLE, "$.store.book.size()");
    assertTrue(size.isNumber());
    assertEquals(4, size.asInt());
  }

  @Test
  void testSizeOnNull() throws Exception {
    JsonNode size = JsonPath.read(SAMPLE, "$.nullField.size()");
    assertTrue(size.isNumber());
    assertEquals(0, size.asInt());
  }

  @Test
  void testLimitOnArray() throws Exception {
    JsonNode limited = JsonPath.read(SAMPLE, "$.store.book.limit(2)");
    assertTrue(limited.isArray());
    assertEquals(2, limited.size());
    assertEquals("Nigel Rees", limited.get(0).get("author").asText());
  }

  @Test
  void testLimitOnString() throws Exception {
    JsonNode limited = JsonPath.read(SAMPLE, "$.store.bicycle.color.limit(2)");
    assertTrue(limited.isTextual());
    assertEquals("re", limited.asText());
  }

  @Test
  void testSortByNumericField() throws Exception {
    JsonNode sorted = JsonPath.read(SAMPLE, "$.store.book.sort($.price)");
    assertTrue(sorted.isArray());
    assertEquals(4, sorted.size());
    assertEquals(8.95, sorted.get(0).get("price").asDouble(), 0.00001);
    assertEquals(22.99, sorted.get(3).get("price").asDouble(), 0.00001);
  }

  @Test
  void testTopLevelExpressionWithDotNotationAndSize() throws Exception {
    String ctx =
        """
      {
        "query_sales_revenue": {
          "success": true,
          "data": [ { "total_revenue": 123.45 } ]
        }
      }
      """;
    JsonNode val =
        JsonPath.read(
            ctx, "$.query_sales_revenue.success == true && $.query_sales_revenue.data.size() > 0");
    assertTrue(val.isBoolean());
    assertTrue(val.asBoolean());
  }

  @Test
  void testPlainPathDotNotationSizeReturnsNumber() throws Exception {
    String ctx =
        """
      {
        "query_sales_revenue": {
          "success": true,
          "data": [ { "total_revenue": 123.45 } ]
        }
      }
      """;
    JsonNode size = JsonPath.read(ctx, "$.query_sales_revenue.data.size()");
    assertTrue(size.isNumber());
    assertTrue(size.asInt() > 0);
  }
}
