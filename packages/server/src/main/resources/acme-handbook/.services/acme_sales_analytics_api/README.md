# ACME Sales Analytics API
  A comprehensive data analytics API for exploring e-commerce sales data.
  This API provides powerful querying capabilities to analyze sales performance,
  customer behavior, product trends, and regional insights.
  
  ## Key Features
  - **Flexible Filtering**: Filter data using field operators (equals, greater than, contains, etc.)
  - **Field Selection**: Choose exactly which fields to include in results
  - **Aggregations**: Calculate sums, averages, counts, min/max values
  - **Real-time Data**: Access to live sales data with historical context
  
  ## Dataset Overview
  The dataset contains sales transactions with the following key entities:
  - **Products**: Electronics, clothing, books, home goods
  - **Customers**: Demographics, location, purchase history
  - **Sales**: Transaction details, pricing, quantities
  - **Regions**: Geographic distribution of sales
  - **Time**: Temporal analysis capabilities
  
  # Data Model - Sales Analytics API
  
  This document describes the complete data model for the Sales Analytics API, including all available fields, their types, and relationships.
  
  ## Entity Relationships
  
  ```
  Customer (1) -----> (N) Sale (N) <----- (1) Product
  |                    |                    |
  |                    |                    |
  v                    v                    v
  Region (1) <----- (N) Customer          Category
  ```
  
  ## Field Categories
  
  ### Sale Fields
  Core transaction data representing individual purchases.
  
  | Field | Type | Description | Example |
  |-------|------|-------------|---------|
  | `sale.id` | string | Unique sale identifier | "SAL-001" |
  | `sale.amount` | number | Total sale amount (USD) | 299.99 |
  | `sale.date` | datetime | When the sale occurred | "2023-12-15T10:30:00Z" |
  | `sale.quantity` | number | Number of items purchased | 2 |
  | `sale.discount` | number | Discount amount applied | 15.00 |
  | `sale.tax` | number | Tax amount | 24.00 |
  | `sale.shipping_cost` | number | Shipping cost | 9.99 |
  | `sale.payment_method` | string | Payment method used | "credit_card" |
  | `sale.status` | string | Sale status | "completed" |
  
  **Sale Status Values:**
  - `pending` - Payment processing
  - `completed` - Successfully processed
  - `cancelled` - Cancelled by customer or system
  - `refunded` - Refunded to customer
  - `failed` - Payment failed
  
  **Payment Method Values:**
  - `credit_card` - Credit card payment
  - `debit_card` - Debit card payment
  - `paypal` - PayPal payment
  - `apple_pay` - Apple Pay
  - `google_pay` - Google Pay
  - `bank_transfer` - Bank transfer
  - `cash` - Cash payment
  
  ### Product Fields
  Information about the items being sold.
  
  | Field | Type | Description | Example |
  |-------|------|-------------|---------|
  | `product.id` | string | Unique product identifier | "PROD-001" |
  | `product.name` | string | Product name | "Wireless Headphones" |
  | `product.category` | string | Main product category | "Electronics" |
  | `product.subcategory` | string | Product subcategory | "Audio" |
  | `product.brand` | string | Product brand | "TechSound" |
  | `product.price` | number | Current selling price | 299.99 |
  | `product.cost` | number | Cost to business | 150.00 |
  | `product.inventory` | number | Current stock level | 45 |
  | `product.rating` | number | Customer rating (1-5) | 4.5 |
  | `product.weight` | number | Product weight (kg) | 0.3 |
  | `product.dimensions` | string | Product dimensions | "20x15x8" |
  
  **Product Categories:**
  - `Electronics` - Electronic devices and accessories
  - `Clothing` - Apparel and fashion items
  - `Home & Garden` - Home improvement and garden supplies
  - `Books` - Books and educational materials
  - `Sports & Outdoors` - Sports equipment and outdoor gear
  - `Health & Beauty` - Health and beauty products
  - `Toys & Games` - Toys and gaming products
  - `Automotive` - Car parts and accessories
  - `Food & Beverages` - Food and drink items
  - `Office Supplies` - Office and stationery items
  
  ### Customer Fields
  Information about the buyers.
  
  | Field | Type | Description | Example |
  |-------|------|-------------|---------|
  | `customer.id` | string | Unique customer identifier | "CUST-001" |
  | `customer.name` | string | Customer full name | "John Smith" |
  | `customer.email` | string | Customer email address | "john@email.com" |
  | `customer.phone` | string | Customer phone number | "+1-555-0123" |
  | `customer.age` | number | Customer age | 34 |
  | `customer.gender` | string | Customer gender | "M" |
  | `customer.city` | string | Customer city | "San Francisco" |
  | `customer.state` | string | Customer state/province | "CA" |
  | `customer.country` | string | Customer country | "USA" |
  | `customer.zip_code` | string | Postal/ZIP code | "94102" |
  | `customer.registration_date` | date | When customer registered | "2022-03-15" |
  | `customer.loyalty_tier` | string | Customer loyalty level | "Gold" |
  | `customer.total_spent` | number | Lifetime spending | 1250.75 |
  | `customer.total_orders` | number | Total number of orders | 8 |
  
  **Gender Values:**
  - `M` - Male
  - `F` - Female
  - `O` - Other
  - `P` - Prefer not to say
  
  **Loyalty Tier Values:**
  - `Bronze` - Basic tier (0-500 spent)
  - `Silver` - Mid tier (500-1500 spent)
  - `Gold` - High tier (1500-5000 spent)
  - `Platinum` - Premium tier (5000+ spent)
  
  ### Regional Fields
  Geographic information for location-based analysis.
  
  | Field | Type | Description | Example |
  |-------|------|-------------|---------|
  | `region.name` | string | Region name | "West Coast" |
  | `region.country` | string | Country name | "United States" |
  | `region.timezone` | string | Timezone | "America/Los_Angeles" |
  | `region.population` | number | Population count | 5000000 |
  
  ### Time Fields
  Temporal analysis fields derived from sale dates.
  
  | Field | Type | Description | Example |
  |-------|------|-------------|---------|
  | `date.year` | number | Year of sale | 2023 |
  | `date.month` | number | Month of sale (1-12) | 12 |
  | `date.quarter` | string | Quarter of sale | "Q4" |
  | `date.week` | number | Week of year (1-53) | 50 |
  | `date.day_of_week` | string | Day of week | "Friday" |
  
  **Quarter Values:**
  - `Q1` - January, February, March
  - `Q2` - April, May, June
  - `Q3` - July, August, September
  - `Q4` - October, November, December
  
  **Day of Week Values:**
  - `Monday`, `Tuesday`, `Wednesday`, `Thursday`, `Friday`, `Saturday`, `Sunday`
  
  ## Indexed Fields
  
  The following fields are indexed for optimal query performance:
  
  ### Primary Indexes
  - `sale.id` - Unique identifier
  - `sale.date` - Temporal queries
  - `customer.id` - Customer lookups
  - `product.id` - Product lookups
  
  ### Secondary Indexes
  - `product.category` - Category filtering
  - `customer.state` - Geographic filtering
  - `sale.amount` - Value-based filtering
  - `customer.loyalty_tier` - Customer segmentation
  
  ### Composite Indexes
  - `(sale.date, product.category)` - Time-series category analysis
  - `(customer.state, sale.date)` - Regional time analysis
  - `(product.category, sale.amount)` - Category value analysis
  
  ## Sample Data Relationships
  
  ### Typical Sale Record
  ```json
  {
  "sale": {
  "id": "SAL-001",
  "amount": 299.99,
  "date": "2023-12-15T10:30:00Z",
  "quantity": 1,
  "discount": 0.00,
  "tax": 24.00,
  "shipping_cost": 9.99,
  "payment_method": "credit_card",
  "status": "completed"
  },
  "product": {
  "id": "PROD-001",
  "name": "Wireless Bluetooth Headphones",
  "category": "Electronics",
  "subcategory": "Audio",
  "brand": "TechSound",
  "price": 299.99,
  "cost": 150.00,
  "inventory": 45,
  "rating": 4.5,
  "weight": 0.3,
  "dimensions": "20x15x8"
  },
  "customer": {
  "id": "CUST-001",
  "name": "John Smith",
  "email": "john.smith@email.com",
  "phone": "+1-555-0123",
  "age": 34,
  "gender": "M",
  "city": "San Francisco",
  "state": "CA",
  "country": "USA",
  "zip_code": "94102",
  "registration_date": "2022-03-15",
  "loyalty_tier": "Gold",
  "total_spent": 1250.75,
  "total_orders": 8
  },
  "date": {
  "year": 2023,
  "month": 12,
  "quarter": "Q4",
  "week": 50,
  "day_of_week": "Friday"
  }
  }
  ```