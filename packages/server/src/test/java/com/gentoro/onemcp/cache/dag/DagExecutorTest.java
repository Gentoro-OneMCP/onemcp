package com.gentoro.onemcp.cache.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test DagExecutor with a simple DAG to verify all node types work.
 */
public class DagExecutorTest {
  
  @Test
  public void testSimpleDagExecution() throws Exception {
    // Create a simple DAG: ConvertValue -> ApiCall -> Filter
    String dagJson = """
        {
          "nodes": [
            {
              "id": "cv1",
              "type": "ConvertValue",
              "config": {
                "conceptualFieldKind": "date_yyyy",
                "targetFormat": "year_int",
                "value": "2024"
              }
            },
            {
              "id": "api1",
              "type": "ApiCall",
              "config": {
                "endpoint": "/sales",
                "method": "GET",
                "params": {
                  "year": "$.cv1.value"
                }
              }
            },
            {
              "id": "filter1",
              "type": "Filter",
              "inputs": {
                "source": "api1"
              },
              "config": {
                "field": "amount",
                "operator": "greater_than",
                "value": "100"
              }
            }
          ],
          "entryPoint": "filter1"
        }
        """;
    
    DagPlan plan = DagPlan.fromJsonString(dagJson);
    
    // Create operation registry with mock API
    OperationRegistry registry = new OperationRegistry();
    registry.register("/sales", (input) -> {
      // Mock API response
      JsonNode data = JacksonUtility.getJsonMapper().createArrayNode()
          .add(JacksonUtility.getJsonMapper().createObjectNode()
              .put("amount", 150.0)
              .put("year", 2024))
          .add(JacksonUtility.getJsonMapper().createObjectNode()
              .put("amount", 50.0)
              .put("year", 2024));
      return data;
    });
    
    DagExecutor executor = new DagExecutor(registry);
    Map<String, String> initialValues = new HashMap<>();
    
    JsonNode result = executor.execute(plan, initialValues);
    
    // Verify result
    assertNotNull(result);
    assertTrue(result.isArray());
    assertEquals(1, result.size()); // Should have 1 item after filtering (amount > 100)
  }
}

