---
title: "Query Examples - Sales Analytics API"
entities:
  - Product
  - Order
  - Category
  - Sales
  - Customer
  - Region
author: "ACME"
---

# Query Examples - Sales Analytics API

This document provides practical examples of how to use the Sales Analytics API for common business scenarios.

## Basic Filtering Examples

### Find High-Value Sales
```json
{
  "filter": [
    {
      "field": "sale.amount",
      "operator": "greater_than",
      "value": 500
    }
  ],
  "fields": [
    "sale.id",
    "sale.amount",
    "product.name",
    "customer.name",
    "sale.date"
  ]
}
```

### Sales in Specific Region
```json
{
  "filter": [
    {
      "field": "customer.state",
      "operator": "equals",
      "value": "CA"
    }
  ],
  "fields": [
    "sale.amount",
    "product.category",
    "customer.city",
    "sale.date"
  ]
}
```

### Electronics Sales in Q4
```json
{
  "filter": [
    {
      "field": "product.category",
      "operator": "equals",
      "value": "Electronics"
    },
    {
      "field": "date.quarter",
      "operator": "equals",
      "value": "Q4"
    }
  ],
  "fields": [
    "sale.id",
    "sale.amount",
    "product.name",
    "sale.date"
  ]
}
```

## Aggregation Examples

### Total Revenue by Category
```json
{
  "filter": [
    {
      "field": "sale.date",
      "operator": "greater_than_or_equal",
      "value": "2023-01-01"
    }
  ],
  "fields": [
    "product.category"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "total_revenue"
    },
    {
      "field": "sale.id",
      "function": "count",
      "alias": "total_sales"
    }
  ]
}
```

### Average Order Value by Customer Age Group
```json
{
  "filter": [
    {
      "field": "sale.status",
      "operator": "equals",
      "value": "completed"
    }
  ],
  "fields": [
    "customer.age"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "avg",
      "alias": "average_order_value"
    },
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "total_spent"
    }
  ]
}
```

### Top 10 Customers by Total Spent
```json
{
  "filter": [
    {
      "field": "sale.date",
      "operator": "greater_than_or_equal",
      "value": "2023-01-01"
    }
  ],
  "fields": [
    "customer.id",
    "customer.name",
    "customer.email"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "total_spent"
    },
    {
      "field": "sale.id",
      "function": "count",
      "alias": "total_orders"
    }
  ],
  "limit": 10
}
```

## Complex Multi-Filter Examples

### Premium Customers in High-Value Categories
```json
{
  "filter": [
    {
      "field": "sale.amount",
      "operator": "greater_than",
      "value": 200
    },
    {
      "field": "product.category",
      "operator": "in",
      "value": ["Electronics", "Home & Garden", "Fashion"]
    },
    {
      "field": "customer.loyalty_tier",
      "operator": "in",
      "value": ["Gold", "Platinum"]
    }
  ],
  "fields": [
    "customer.name",
    "customer.email",
    "product.category",
    "sale.amount",
    "sale.date"
  ]
}
```

### Seasonal Analysis - Holiday Sales
```json
{
  "filter": [
    {
      "field": "date.month",
      "operator": "in",
      "value": [11, 12]
    },
    {
      "field": "sale.amount",
      "operator": "greater_than",
      "value": 100
    }
  ],
  "fields": [
    "date.month",
    "product.category",
    "customer.state"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "monthly_revenue"
    },
    {
      "field": "sale.id",
      "function": "count",
      "alias": "monthly_orders"
    }
  ]
}
```

### Inventory Performance Analysis
```json
{
  "filter": [
    {
      "field": "product.inventory",
      "operator": "less_than",
      "value": 50
    },
    {
      "field": "sale.date",
      "operator": "greater_than_or_equal",
      "value": "2023-11-01"
    }
  ],
  "fields": [
    "product.name",
    "product.category",
    "product.inventory",
    "product.price"
  ],
  "aggregates": [
    {
      "field": "sale.quantity",
      "function": "sum",
      "alias": "units_sold"
    },
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "revenue_generated"
    }
  ]
}
```

## Geographic Analysis Examples

### Regional Performance Comparison
```json
{
  "filter": [
    {
      "field": "sale.date",
      "operator": "between",
      "value": ["2023-01-01", "2023-12-31"]
    }
  ],
  "fields": [
    "customer.state",
    "customer.country"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "state_revenue"
    },
    {
      "field": "sale.id",
      "function": "count",
      "alias": "state_orders"
    },
    {
      "field": "customer.id",
      "function": "count",
      "alias": "unique_customers"
    }
  ]
}
```

### Shipping Cost Analysis by Region
```json
{
  "filter": [
    {
      "field": "sale.shipping_cost",
      "operator": "greater_than",
      "value": 0
    }
  ],
  "fields": [
    "customer.state",
    "product.weight"
  ],
  "aggregates": [
    {
      "field": "sale.shipping_cost",
      "function": "avg",
      "alias": "avg_shipping_cost"
    },
    {
      "field": "sale.shipping_cost",
      "function": "sum",
      "alias": "total_shipping_revenue"
    }
  ]
}
```

## Customer Segmentation Examples

### Customer Lifetime Value Analysis
```json
{
  "filter": [
    {
      "field": "customer.registration_date",
      "operator": "greater_than_or_equal",
      "value": "2022-01-01"
    }
  ],
  "fields": [
    "customer.loyalty_tier",
    "customer.age",
    "customer.gender"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "lifetime_value"
    },
    {
      "field": "sale.id",
      "function": "count",
      "alias": "total_orders"
    },
    {
      "field": "sale.amount",
      "function": "avg",
      "alias": "average_order_value"
    }
  ]
}
```

### New vs Returning Customer Analysis
```json
{
  "filter": [
    {
      "field": "sale.date",
      "operator": "greater_than_or_equal",
      "value": "2023-01-01"
    }
  ],
  "fields": [
    "customer.total_orders",
    "customer.registration_date"
  ],
  "aggregates": [
    {
      "field": "sale.amount",
      "function": "sum",
      "alias": "total_spent"
    }
  ]
}
```

## Tips for Effective Queries

1. **Start Simple**: Begin with basic filters and gradually add complexity
2. **Use Indexed Fields**: Fields like `sale.date`, `product.category`, and `customer.state` are indexed for faster queries
3. **Limit Results**: Always use the `limit` parameter for large datasets
4. **Combine Filters**: Use multiple filters to narrow down results effectively
5. **Use Aggregates**: Aggregations are more efficient than processing large result sets
6. **Consider Time Ranges**: Always include date filters for time-sensitive analysis

## Common Pitfalls

1. **Too Broad Filters**: Avoid queries that return millions of records
2. **Missing Date Filters**: Always include time constraints for performance
3. **Over-aggregation**: Don't aggregate every field - focus on key metrics
4. **Ignoring Limits**: Set appropriate limits to avoid timeouts
5. **Complex Nested Filters**: Keep filter logic simple and readable
