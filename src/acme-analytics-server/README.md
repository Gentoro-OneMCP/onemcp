# ACME Analytics Server

A mock analytics server for testing the OneMCP system. This server provides a realistic data analytics API that the OneMCP can learn from and interact with.

## Overview

The ACME Analytics Server simulates a comprehensive e-commerce analytics platform with:

- **Sales Analytics API**: Query interface for sales data analysis
- **Rich Dataset**: Products, customers, sales transactions, and regional data
- **Business Rules**: Realistic policies and optimization guidelines
- **Comprehensive Documentation**: Complete API specs, examples, and use cases

## Directory Structure

```
acme-analytics-server/
├── README.md                    # This file
├── onemcp-handbook/          # Handbook for OneMCP
│   ├── docs/                   # Documentation
│   ├── openapi/                # API specifications
│   ├── config/                 # Configuration files
│   └── examples/               # Sample queries & use cases
└── server/                     # Java server implementation
    ├── pom.xml                 # Maven project file
    ├── src/                    # Java source code
    │   ├── main/java/com/acme/server/AcmeServer.java
    │   ├── main/java/com/acme/server/FakeDataGenerator.java
    │   └── test/java/com/acme/server/AcmeServerTest.java
    └── build.sh                # Build script
```

## OneMCP Handbook

The `onemcp-handbook/` directory contains everything the OneMCP needs to understand and interact with the ACME Analytics API:

### Key Components

- **OpenAPI Specification**: Complete API definition with examples
- **Data Model**: Detailed field descriptions and relationships
- **Query Examples**: Real-world use cases and sample queries
- **Configuration**: API settings and runtime configuration

### Mock Service: ACME Sales Analytics API

The onemcp-handbook includes a comprehensive mock service with:

- **Flexible Query Interface**: Filter, select fields, and perform aggregations
- **Rich Dataset**: 4 entity types (Sales, Products, Customers, Regions) with 30+ fields
- **Real-world Complexity**: Multiple categories, payment methods, loyalty tiers
- **Business Logic**: Comprehensive logic for data access and optimization

## Getting Started

### Build the Server

Navigate to the `server/` directory and build the project using Maven:

```bash
cd src/acme-analytics-server/server
./build.sh
```

This will compile the Java code and create an executable JAR file in `target/`.

### Run the Server

You can run the server directly from the command line:

```bash
java -jar target/acme-analytics-server-1.0.0.jar [port]
```
(e.g., `java -jar target/acme-analytics-server-1.0.0.jar 8080`)

### Docker Entry Point

The server is designed to be invoked by external Docker containers. The entry point is:

```bash
java -jar target/acme-analytics-server-1.0.0.jar [port]
```

**Parameters:**
- `port` (optional): Port number to run the server on (default: 8080)

**Example Dockerfile usage:**
```dockerfile
FROM openjdk:11-jdk-slim
WORKDIR /app
COPY target/acme-analytics-server-1.0.0.jar .
EXPOSE 8080
CMD ["java", "-jar", "acme-analytics-server-1.0.0.jar", "8080"]
```

### Test the Server

The server includes a JUnit test suite. To run tests:

```bash
cd src/acme-analytics-server/server
mvn test
```

## Example Natural Language → API Call

**User Prompt**: "Show me the total revenue for electronics in California last quarter"

**OneMCP Understanding**:
- Entity: Electronics (product.category)
- Location: California (customer.state)
- Time: Last quarter (date.quarter)
- Aggregation: Total revenue (sum of sale.amount)

**Generated Query**:
```json
{
  "filter": [
    {"field": "product.category", "operator": "equals", "value": "Electronics"},
    {"field": "customer.state", "operator": "equals", "value": "CA"},
    {"field": "date.quarter", "operator": "equals", "value": "Q4"}
  ],
  "fields": ["product.category", "customer.state"],
  "aggregates": [
    {"field": "sale.amount", "function": "sum", "alias": "total_revenue"}
  ]
}
```

## API Endpoints

The server provides three main endpoints:

- **`GET /health`**: Health check endpoint
- **`GET /fields`**: Get available fields for querying
- **`POST /query`**: Query sales data with filters, field selection, and aggregations

## Development

### Adding New Features

1. **Update OpenAPI Spec**: Add new endpoints, fields, or capabilities in `onemcp-handbook/openapi/`
2. **Update Documentation**: Add examples and use cases in `onemcp-handbook/docs/`
3. **Update Examples**: Add sample queries in `onemcp-handbook/examples/`
4. **Update Server Code**: Modify Java implementation in `server/src/main/java/`
5. **Rebuild**: Run `./build.sh` to compile changes

### Testing

The server includes comprehensive JUnit tests covering:

- Server lifecycle management
- Health check functionality
- Field listing
- Basic query operations
- Aggregation queries
- Complex multi-filter queries
- Error handling
- Invalid query scenarios

## Integration with OneMCP

OneMCP should be configured to ingest the OpenAPI specification and other documentation located in the `onemcp-handbook/` directory. This will enable the agent to understand the API's capabilities and generate appropriate queries from natural language prompts.

OneMCP uses this onemcp-handbook to:

1. **Understand Entities**: Recognize products, customers, sales, regions
2. **Map Relationships**: Connect related data across entities
3. **Apply Business Logic**: Respect policies and optimization guidelines
4. **Generate Queries**: Convert natural language to structured API calls
5. **Handle Errors**: Provide helpful feedback for invalid requests
6. **Follow Guidelines**: Adhere to behavioral instructions and best practices

## Support

For questions about this mock server:
- Review the documentation in `onemcp-handbook/docs/`
- Check sample queries in `onemcp-handbook/examples/`
- Examine configuration in `onemcp-handbook/config/`

## License

This mock server is part of the OneMCP project and follows the same licensing terms.
