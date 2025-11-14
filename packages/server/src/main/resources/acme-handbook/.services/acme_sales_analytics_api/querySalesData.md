# querySalesData
----------------------------------------
| Property        | Value              |
|-----------------|--------------------|
| Method          | POST |
| Relative Path   | /query   |
----------------------------------------
      
```md
Query Sales Data

Execute complex queries against the sales analytics dataset. This endpoint
allows you to filter, select fields, and perform aggregations on sales data.

## Query Structure
- **filter**: Array of filter conditions to narrow down results
- **fields**: Array of field names to include in the response
- **aggregates**: Optional aggregations to perform on the data

## Example Use Cases
- Find all electronics sales in Q4 2023
- Calculate total revenue by region for last month
- Get top 10 customers by purchase amount
- Analyze product performance by category


```

### Request Body Schema
```json5
Object({
  // (Required): Array of filter conditions to apply to the data
  "filter": Array(
    Object({
      // (Required): Name of the field to filter on
      "field": String(Enum("sale.id", "sale.amount", "sale.date", "sale.quantity", "sale.discount", "sale.tax", "sale.shipping_cost", "sale.payment_method", "sale.status", "product.id", "product.name", "product.category", "product.subcategory", "product.brand", "product.price", "product.cost", "product.inventory", "product.rating", "product.weight", "product.dimensions", "customer.id", "customer.name", "customer.email", "customer.phone", "customer.age", "customer.gender", "customer.city", "customer.state", "customer.country", "customer.zip_code", "customer.registration_date", "customer.loyalty_tier", "customer.total_spent", "customer.total_orders", "region.name", "region.country", "region.timezone", "region.population", "date.year", "date.month", "date.quarter", "date.week", "date.day_of_week")),
      // (Required): Comparison operator to use
      "operator": String(Enum("equals", "not_equals", "greater_than", "greater_than_or_equal", "less_than", "less_than_or_equal", "contains", "not_contains", "starts_with", "ends_with", "in", "not_in", "between", "is_null", "is_not_null")),
      // (Optional): Value(s) to compare against. Optional for `is_null` and `is_not_null`. For `between`, provide an array with exactly two values.
      "value": OneOf(String, Number, Boolean, Array(OneOf(String, Number, Boolean)))
    })
  ),
  // (Required): Array of field names to include in the response
  "fields": Array(
    String
  ),
  // (Optional): Optional array of aggregation functions to apply
  "aggregates": Array(
    Object({
      // (Required): Field to perform aggregation on
      "field": String,
      // (Required): Aggregation function to apply
      "function": String(Enum("sum", "avg", "count", "min", "max", "median", "stddev", "variance")),
      // (Required): Name for the aggregated result column
      "alias": String
    })
  ),
  // (Optional): Maximum number of records to return
  "limit": Integer,
  // (Optional): Number of records to skip for pagination
  "offset": Integer
})
```

### Example Request
```json
{
  "body": { ... }
}
```

### Response `200`
Query executed successfully

```json5
Object({
  // (Optional): Whether the query was successful
  "success": Boolean,
  // (Optional): Array of query results
  "data": Array(
    // Flexible object structure based on requested fields
    Object({additionalProperties})
  ),
  // (Optional): 
  "metadata": Object({
    // (Optional): Total number of records matching the query
    "total_records": Integer,
    // (Optional): Query execution time in milliseconds
    "execution_time_ms": Integer,
    // (Optional): Unique identifier for this query execution
    "query_id": String,
    // (Optional): Whether there are more results available
    "has_more": Boolean
  })
})
```

### Response `400`
Invalid query request

```json5
Object({
  // (Optional): 
  "success": Boolean,
  // (Optional): 
  "error": Object({
    // (Optional): Error code
    "code": String,
    // (Optional): Human-readable error message
    "message": String,
    // (Optional): Additional error details
    "details": Object({
    })
  }),
  // (Optional): When the error occurred
  "timestamp": String(date-time)
})
```

### Response `500`
Internal server error

```json5
Object({
  // (Optional): 
  "success": Boolean,
  // (Optional): 
  "error": Object({
    // (Optional): Error code
    "code": String,
    // (Optional): Human-readable error message
    "message": String,
    // (Optional): Additional error details
    "details": Object({
    })
  }),
  // (Optional): When the error occurred
  "timestamp": String(date-time)
})
```

