package engine;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.engine.ExecutionPlanEngine;
import com.gentoro.onemcp.engine.OperationRegistry;
import com.gentoro.onemcp.utility.JacksonUtility;
import org.junit.jupiter.api.Test;

/**
 * Verifies that an execution plan using querySalesData routes to the expected completion node when
 * the operation returns a successful payload with data.
 */
public class QuerySalesRevenueRoutingTest {

  private static final String PLAN =
      """
    {
      "start_node" : {
        "vars" : {
          "year_filter" : [ 2024, 2024 ]
        },
        "route" : "query_sales_revenue"
      },
      "query_sales_revenue" : {
        "operation" : "querySalesData",
        "input" : {
          "fields" : [ ],
          "aggregates" : [ {
            "field" : "sale.amount",
            "function" : "sum",
            "alias" : "total_revenue"
          } ],
          "filter" : [ {
            "field" : "date.year",
            "operator" : "between",
            "value" : "$.start_node.year_filter"
          } ]
        },
        "route" : [ {
          "condition" : "$.query_sales_revenue.success == true && $.query_sales_revenue.data.size() > 0",
          "node" : "completion_success"
        }, "completion_no_data" ]
      },
      "completion_success" : {
        "completed" : true,
        "vars" : {
          "total_revenue" : "$.query_sales_revenue.data[0].total_revenue"
        }
      },
      "completion_no_data" : {
        "completed" : true,
        "vars" : {
          "message" : "No sales data found for the specified year."
        }
      }
    }
    """;

  private static final String MOCK_RESULT =
      """
    {
      "success" : true,
      "data" : [ {
        "total_revenue" : 5640255.26
      } ],
      "metadata" : {
        "total_records" : 4960,
        "execution_time_ms" : 43,
        "query_id" : "qry_16bc66be",
        "has_more" : false
      }
    }
    """;

  @Test
  void routesToCompletionSuccess() throws Exception {
    ObjectMapper mapper = JacksonUtility.getJsonMapper();
    JsonNode plan = mapper.readTree(PLAN);

    // Prepare registry and mock operation
    OperationRegistry registry =
        new OperationRegistry()
            .register(
                "querySalesData",
                input -> {
                  try {
                    return mapper.readTree(MOCK_RESULT);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });

    ExecutionPlanEngine engine = new ExecutionPlanEngine(mapper, registry);

    JsonNode out = engine.execute(plan, null);

    assertNotNull(out, "Engine should return a terminal vars object");
    assertTrue(
        out.has("total_revenue"), "Should route to completion_success and expose total_revenue");
    assertTrue(out.get("total_revenue").isNumber(), "total_revenue must be numeric");
    assertEquals(
        5640255.26,
        out.get("total_revenue").asDouble(),
        0.00001,
        "total_revenue must match mocked operation output");
  }
}
