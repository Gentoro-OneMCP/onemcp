package com.gentoro.onemcp.acme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.utility.JacksonUtility;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.eclipse.jetty.ee10.servlet.ServletHolder;

/** ACME Analytics Server - Jetty Implementation */
public class AcmeServer {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(AcmeServer.class);
  private final FakeDataGenerator dataGenerator;
  private final Map<String, List<Map<String, Object>>> dataStore;
  private final OneMcp oneMcp;

  // === Field definitions ===
  private static final Map<String, String> FIELD_TYPES = new HashMap<>();

  static {
    // Sale fields
    FIELD_TYPES.put("sale.id", "string");
    FIELD_TYPES.put("sale.amount", "number");
    FIELD_TYPES.put("sale.date", "datetime");
    FIELD_TYPES.put("sale.quantity", "number");
    FIELD_TYPES.put("sale.discount", "number");
    FIELD_TYPES.put("sale.tax", "number");
    FIELD_TYPES.put("sale.shipping_cost", "number");
    FIELD_TYPES.put("sale.payment_method", "string");
    FIELD_TYPES.put("sale.status", "string");

    // Product fields
    FIELD_TYPES.put("product.id", "string");
    FIELD_TYPES.put("product.name", "string");
    FIELD_TYPES.put("product.category", "string");
    FIELD_TYPES.put("product.subcategory", "string");
    FIELD_TYPES.put("product.brand", "string");
    FIELD_TYPES.put("product.price", "number");
    FIELD_TYPES.put("product.cost", "number");
    FIELD_TYPES.put("product.inventory", "number");
    FIELD_TYPES.put("product.rating", "number");
    FIELD_TYPES.put("product.weight", "number");
    FIELD_TYPES.put("product.dimensions", "string");

    // Customer fields
    FIELD_TYPES.put("customer.id", "string");
    FIELD_TYPES.put("customer.name", "string");
    FIELD_TYPES.put("customer.email", "string");
    FIELD_TYPES.put("customer.phone", "string");
    FIELD_TYPES.put("customer.age", "number");
    FIELD_TYPES.put("customer.gender", "string");
    FIELD_TYPES.put("customer.city", "string");
    FIELD_TYPES.put("customer.state", "string");
    FIELD_TYPES.put("customer.country", "string");
    FIELD_TYPES.put("customer.zip_code", "string");
    FIELD_TYPES.put("customer.registration_date", "date");
    FIELD_TYPES.put("customer.loyalty_tier", "string");
    FIELD_TYPES.put("customer.total_spent", "number");
    FIELD_TYPES.put("customer.total_orders", "number");

    // Time fields
    FIELD_TYPES.put("date.year", "number");
    FIELD_TYPES.put("date.month", "number");
    FIELD_TYPES.put("date.quarter", "string");
    FIELD_TYPES.put("date.week", "number");
    FIELD_TYPES.put("date.day_of_week", "string");
  }

  public AcmeServer(OneMcp oneMcp) {
    this.dataGenerator = new FakeDataGenerator();
    this.dataStore = new ConcurrentHashMap<>();
    this.oneMcp = oneMcp;
  }

  private String contextPath() {
    String contextPath = oneMcp.configuration().getString("http.acme.context-path", "/acme");
    if (contextPath == null || !contextPath.startsWith("/")) {
      throw new IllegalArgumentException("Invalid context path: " + contextPath);
    }
    return contextPath;
  }

  /** Register ACME servlets on the provided shared Jetty Server. */
  public void register() {
    generateFakeData();
    oneMcp
        .httpServer()
        .getContextHandler()
        .addServlet(new ServletHolder(new HealthServlet()), "%s/health".formatted(contextPath()));
    oneMcp
        .httpServer()
        .getContextHandler()
        .addServlet(new ServletHolder(new FieldsServlet()), "%s/fields".formatted(contextPath()));
    oneMcp
        .httpServer()
        .getContextHandler()
        .addServlet(new ServletHolder(new QueryServlet()), "%s/query".formatted(contextPath()));
    log.info("ACME Analytics servlets registered under {}/*", contextPath());
  }

  private void generateFakeData() {
    log.info("Generating fake data...");
    List<Map<String, Object>> customers = dataGenerator.generateCustomers(1000);
    List<Map<String, Object>> products = dataGenerator.generateProducts(500);
    List<Map<String, Object>> sales = dataGenerator.generateSales(10000, customers, products);
    dataStore.put("customers", customers);
    dataStore.put("products", products);
    dataStore.put("sales", sales);
    log.info(
        "Generated {} customers, {} products, {} sales",
        customers.size(),
        products.size(),
        sales.size());

    dumpDataSource(customers, "customers.csv");
    dumpDataSource(products, "products.csv");
    dumpDataSource(sales, "sales.csv");
  }

  private void dumpDataSource(List<Map<String, Object>> data, String fileName) {
    try {
      Path file = Files.createTempFile(fileName, ".csv");
      String header = String.join(",", data.getFirst().keySet());
      List<String> lines = new ArrayList<>();
      lines.add(header);
      for (Map<String, Object> row : data) {
        lines.add(row.values().stream().map(v -> "\"" + v + "\"").collect(Collectors.joining(",")));
      }
      Files.write(file, lines);
      log.info("Data source saved at {}", file.toAbsolutePath());
    } catch (IOException e) {
      log.error("Failed to dump data source {}", fileName, e);
    }
  }

  // === SERVLETS ===

  private class HealthServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      ObjectNode response = JacksonUtility.getJsonMapper().createObjectNode();
      response.put("status", "healthy");
      response.put("timestamp", new Date().toString());
      response.put("version", "1.0.0");
      sendJson(resp, 200, response);
    }
  }

  private class FieldsServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      ObjectNode response = JacksonUtility.getJsonMapper().createObjectNode();
      ArrayNode fields = response.putArray("fields");

      FIELD_TYPES.forEach(
          (name, type) -> {
            ObjectNode field = fields.addObject();
            field.put("name", name);
            field.put("type", type);
            field.put("description", getFieldDescription(name));
            field.put("category", getFieldCategory(name));
            field.put("nullable", true);
            field.put("example", getFieldExample(name));
          });

      sendJson(resp, 200, response);
    }
  }

  private class QueryServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      try {
        ObjectNode request =
            (ObjectNode) JacksonUtility.getJsonMapper().readTree(req.getInputStream());
        log.info("Received query request: {}", request);

        if (!request.has("filter") || !request.has("fields")) {
          sendError(resp, 400, "Missing required fields: filter and fields");
          return;
        }

        QueryResult result = processQuery(request);
        ObjectNode response = JacksonUtility.getJsonMapper().createObjectNode();
        response.put("success", true);
        response.set("data", result.data);
        response.set("metadata", result.metadata);
        sendJson(resp, 200, response);

      } catch (Exception e) {
        e.printStackTrace();
        sendError(resp, 500, "Internal server error: " + e.getMessage());
      }
    }
  }

  /** Process a query request */
  private QueryResult processQuery(ObjectNode request) {
    List<Map<String, Object>> sales = dataStore.get("sales");
    List<Map<String, Object>> customers = dataStore.get("customers");
    List<Map<String, Object>> products = dataStore.get("products");

    // Join with related data
    List<Map<String, Object>> enrichedData = enrichData(sales, customers, products);

    // Apply filters
    List<Map<String, Object>> filteredSales = applyFilters(enrichedData, request.get("filter"));

    // Select fields
    List<String> requestedFields = new ArrayList<>();
    if (request.has("fields") && request.get("fields").isArray()) {
      ArrayNode fieldsArray = (ArrayNode) request.get("fields");
      for (int i = 0; i < fieldsArray.size(); i++) {
        requestedFields.add(fieldsArray.get(i).asText());
      }
    }

    List<Map<String, Object>> selectedData =
        requestedFields.isEmpty()
            ? Collections.emptyList()
            : selectFields(filteredSales, requestedFields);

    // Apply limit
    int limit = request.has("limit") ? request.get("limit").asInt() : 1000;
    if (selectedData.size() > limit) {
      selectedData = selectedData.subList(0, limit);
    }

    // Process aggregates
    JsonNode data = null;
    if (request.has("aggregates")) {
      data = processAggregates(filteredSales, requestedFields, request.get("aggregates"));
    } else {
      data = JacksonUtility.getJsonMapper().valueToTree(selectedData);
    }

    // Create response
    QueryResult result = new QueryResult();
    result.data = data;

    // Create metadata
    ObjectNode metadata = JacksonUtility.getJsonMapper().createObjectNode();
    metadata.put("total_records", filteredSales.size());
    metadata.put("execution_time_ms", ThreadLocalRandom.current().nextInt(10, 100));
    metadata.put("query_id", "qry_" + UUID.randomUUID().toString().substring(0, 8));
    metadata.put("has_more", ((ArrayNode) data).size() >= limit);
    result.metadata = metadata;

    return result;
  }

  /** Apply filters to the data */
  private List<Map<String, Object>> applyFilters(
      List<Map<String, Object>> data, JsonNode filters) {
    if (filters == null || !filters.isArray()) {
      return data;
    }

    List<Map<String, Object>> filtered = new ArrayList<>(data);

    for (JsonNode filter : filters) {
      String field = filter.get("field").asText();
      String operator = filter.get("operator").asText();
      JsonNode value = filter.get("value");

      filtered =
          filtered.stream()
              .filter(record -> evaluateFilter(record, field, operator, value))
              .collect(Collectors.toList());
    }

    return filtered;
  }

  /** Evaluate a single filter condition */
  private boolean evaluateFilter(
      Map<String, Object> record,
      String field,
      String operator,
      JsonNode value) {
    Object fieldValue = getNestedValue(record, field);

    if (fieldValue == null) {
      return "is_null".equals(operator);
    }

    switch (operator) {
      case "equals":
        return Objects.equals(fieldValue.toString(), value.asText());
      case "not_equals":
        return !Objects.equals(fieldValue.toString(), value.asText());
      case "greater_than":
        return compareNumbers(field, fieldValue, value) > 0;
      case "greater_than_or_equal":
        return compareNumbers(field, fieldValue, value) >= 0;
      case "less_than":
        return compareNumbers(field, fieldValue, value) < 0;
      case "less_than_or_equal":
        return compareNumbers(field, fieldValue, value) <= 0;
      case "contains":
        return fieldValue.toString().toLowerCase().contains(value.asText().toLowerCase());
      case "not_contains":
        return !fieldValue.toString().toLowerCase().contains(value.asText().toLowerCase());
      case "in":
        if (value.isArray()) {
          for (JsonNode item : value) {
            if (Objects.equals(fieldValue.toString(), item.asText())) {
              return true;
            }
          }
          return false;
        }
        return false;
      case "between":
        if (value.isArray() && value.size() == 2) {
          double fieldNum = toNumber(field, fieldValue);
          double min = toNumber(field, getJsonNodeValue(value.get(0)));
          double max = toNumber(field, getJsonNodeValue(value.get(1)));
          return fieldNum >= min && fieldNum <= max;
        }
        return false;
      case "is_not_null":
        return fieldValue != null;
      default:
        return true;
    }
  }

  private Object getJsonNodeValue(JsonNode node) {
    if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isNumber()) {
      return node.asDouble();
    } else if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      for (JsonNode item : node) {
        list.add(getJsonNodeValue(item));
      }
      return list;
    }
    throw new IllegalArgumentException("Invalid JSON node type: " + node.getNodeType());
  }

  /** Get nested value from record using dot notation */
  private Object getNestedValue(Map<String, Object> record, String field) {
    String[] parts = field.split("\\.");
    Object current = record;
    boolean found = false;
    for (String part : parts) {
      if (current instanceof Map) {
        current = ((Map<?, ?>) current).get(part);
        found = true;
      } else {
        current = record.get(part);
        found = true;
        break;
      }
    }
    if (!found) {
      throw new IllegalArgumentException("Invalid field: " + field);
    }

    return Objects.requireNonNullElse(current, "[NULL]");
  }

  /** Compare two values as numbers */
  private int compareNumbers(
      String field, Object fieldValue, JsonNode value) {
    double fieldNum = toNumber(field, fieldValue);
    double valueNum = toNumber(field, getJsonNodeValue(value));
    return Double.compare(fieldNum, valueNum);
  }

  /** Convert value to number */
  private double toNumber(String fieldName, Object value) {
    String fieldType = FIELD_TYPES.get(fieldName);
    if (fieldType == null) {
      throw new IllegalArgumentException("Unknown field: " + fieldName);
    }
    switch (fieldType) {
      case "datetime":
        if (value instanceof String) {
          try {
            return LocalDateTime.parse(value.toString()).toLocalDate().toEpochDay();
          } catch (java.time.format.DateTimeParseException e) {
            try {
              return LocalDate.parse(value.toString()).toEpochDay();
            } catch (java.time.format.DateTimeParseException e2) {
              return FlexibleDateParser.parseFlexible(value.toString()).toEpochMilli();
            }
          }
        } else if (value instanceof LocalDate) {
          return ((LocalDate) value).toEpochDay();
        }
        break;
      case "number":
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        } else if (value instanceof String) {
          try {
            return Double.parseDouble(value.toString());
          } catch (NumberFormatException e) {
            return 0.0;
          }
        }
        break;
      default:
        break;
    }
    throw new IllegalArgumentException("Invalid field type: " + fieldType);
  }

  /** Enrich sales data with customer and product information */
  private List<Map<String, Object>> enrichData(
      List<Map<String, Object>> sales,
      List<Map<String, Object>> customers,
      List<Map<String, Object>> products) {
    Map<String, Map<String, Object>> customerMap =
        customers.stream().collect(Collectors.toMap(c -> (String) c.get("id"), c -> c));
    Map<String, Map<String, Object>> productMap =
        products.stream().collect(Collectors.toMap(p -> (String) p.get("id"), p -> p));

    return sales.stream()
        .map(
            sale -> {
              Map<String, Object> enriched = new HashMap<>();

              enriched.put("sale", sale);

              // Add customer data
              String customerId = (String) sale.get("customer_id");
              if (customerId != null && customerMap.containsKey(customerId)) {
                enriched.put("customer", customerMap.get(customerId));
              }

              // Add product data
              String productId = (String) sale.get("product_id");
              if (productId != null && productMap.containsKey(productId)) {
                enriched.put("product", productMap.get(productId));
              }

              // Add time fields
              String saleDate = (String) sale.get("date");
              if (saleDate != null) {
                enriched.put("date", extractTimeFields(saleDate));
              }

              return enriched;
            })
        .collect(Collectors.toList());
  }

  /** Extract time fields from date string */
  private Map<String, Object> extractTimeFields(String dateStr) {
    Map<String, Object> timeFields = new HashMap<>();
    try {
      // Simple date parsing - assuming format like "2023-12-15"
      String[] parts = dateStr.split("-");
      if (parts.length >= 3) {
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);

        timeFields.put("year", year);
        timeFields.put("month", month);
        timeFields.put("quarter", "Q" + ((month - 1) / 3 + 1));
        timeFields.put("week", (month - 1) * 4 + (day - 1) / 7 + 1);
        timeFields.put("day_of_week", getDayOfWeek(day));
      }
    } catch (Exception e) {
      // Default values if parsing fails
      timeFields.put("year", 2023);
      timeFields.put("month", 12);
      timeFields.put("quarter", "Q4");
      timeFields.put("week", 50);
      timeFields.put("day_of_week", "Friday");
    }
    return timeFields;
  }

  /** Get day of week (simplified) */
  private String getDayOfWeek(int day) {
    String[] days = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    return days[day % 7];
  }

  /** Select only requested fields from the data */
  private List<Map<String, Object>> selectFields(
      List<Map<String, Object>> data, List<String> requestedFields) {
    return data.stream()
        .map(
            record -> {
              Map<String, Object> selected = new HashMap<>();
              for (String field : requestedFields) {
                Object value = getNestedValue(record, field);
                if (value != null) {
                  selected.put(field, value);
                }
              }
              return selected;
            })
        .collect(Collectors.toList());
  }

  /** Process aggregation functions */
  private ArrayNode processAggregates(
      List<Map<String, Object>> data,
      List<String> requestedFields,
      JsonNode aggregates) {
    ArrayNode result = JacksonUtility.getJsonMapper().createArrayNode();

    Set<String> aggregateFieldsSet = new HashSet<>();
    Set<String> requestedFieldsSet = new HashSet<>();
    Set<String> selectedFieldsSet = new HashSet<>();
    for (JsonNode aggregate : aggregates) {
      String field = aggregate.get("field").asText();
      selectedFieldsSet.add(field);
      aggregateFieldsSet.add(field);
    }

    requestedFields.forEach(
        field -> {
          selectedFieldsSet.add(field);
          if (!aggregateFieldsSet.contains(field)) {
            requestedFieldsSet.add(field);
          }
        });

    List<Map<String, Object>> localData = new ArrayList<>();
    data.forEach(
        record -> {
          Map<String, Object> selected = new HashMap<>();
          for (String field : selectedFieldsSet) {
            selected.put(field, getNestedValue(record, field));
          }
          localData.add(selected);
        });

    while (true) {
      if (localData.isEmpty()) {
        break;
      }

      final Map<String, Object> record = localData.getFirst();
      List<Map<String, Object>> dataToAggregate = new ArrayList<>();
      int rowIndex = 0;
      while (rowIndex < localData.size()) {
        final int finalRowIndex = rowIndex;
        boolean allMatch =
            requestedFieldsSet.isEmpty()
                || requestedFieldsSet.stream()
                    .allMatch(
                        field ->
                            Objects.equals(
                                record.get(field), localData.get(finalRowIndex).get(field)));
        if (allMatch) {
          Map<String, Object> aggregatedRecord = new HashMap<>();
          for (JsonNode aggregate : aggregates) {
            String field = aggregate.get("field").asText();
            aggregatedRecord.put(field, localData.get(finalRowIndex).get(field));
          }
          dataToAggregate.add(aggregatedRecord);
          localData.remove(finalRowIndex);
        } else {
          rowIndex++;
        }
      }

      ObjectNode recordNode = JacksonUtility.getJsonMapper().createObjectNode();
      requestedFieldsSet.forEach(
          (field) -> {
            Object value = record.get(field);
            if (value != null) {
              if (value instanceof String) {
                recordNode.put(field, value.toString());
              } else if (value instanceof Number) {
                recordNode.put(field, ((Number) value).doubleValue());
              } else if (value instanceof Boolean) {
                recordNode.put(field, (Boolean) value);
              } else if (value instanceof LocalDate) {
                recordNode.put(field, ((LocalDate) value).toEpochDay());
              } else if (value instanceof LocalDateTime) {
                recordNode.put(field, ((LocalDateTime) value).toLocalDate().toEpochDay());
              } else {
                throw new IllegalArgumentException(
                    "Unsupported field type: " + value.getClass().getName());
              }
            } else {
              recordNode.putNull(field);
            }
          });

      for (JsonNode aggregate : aggregates) {
        String field = aggregate.get("field").asText();
        String function = aggregate.get("function").asText();
        String alias = aggregate.get("alias").asText();

        List<Object> values =
            dataToAggregate.stream()
                .map(aggRecord -> aggRecord.get(field))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        double aggregateValue = calculateAggregate(field, values, function);
        recordNode.put(alias, aggregateValue);
      }

      result.add(recordNode);
    }

    return result;
  }

  /** Calculate aggregate value */
  private double calculateAggregate(String field, List<Object> values, String function) {
    if (values.isEmpty()) {
      return 0.0;
    }

    List<Double> numbers;
    if (function.equals("count")) {
      numbers = values.stream().map(e -> 1.0).collect(Collectors.toList());
    } else {
      numbers = values.stream().map(e -> this.toNumber(field, e)).collect(Collectors.toList());
    }
    switch (function) {
      case "sum":
        return numbers.stream().mapToDouble(Double::doubleValue).sum();
      case "avg":
        return numbers.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      case "count":
        return values.size();
      case "min":
        return numbers.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
      case "max":
        return numbers.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
      case "median":
        Collections.sort(numbers);
        int size = numbers.size();
        if (size % 2 == 0) {
          return (numbers.get(size / 2 - 1) + numbers.get(size / 2)) / 2.0;
        } else {
          return numbers.get(size / 2);
        }
      default:
        return 0.0;
    }
  }

  private void sendJson(HttpServletResponse resp, int code, Object response) throws IOException {
    String json = JacksonUtility.toJson(response);
    log.debug("Sending response ({}):\n{}\n---\n", code, json);
    resp.setStatus(code);
    resp.setContentType("application/json");
    try (PrintWriter out = resp.getWriter()) {
      out.println(json);
    }
  }

  private void sendError(HttpServletResponse resp, int code, String msg) throws IOException {
    ObjectNode error = JacksonUtility.getJsonMapper().createObjectNode();
    error.put("success", false);
    error.put("error", msg);
    error.put("timestamp", new Date().toString());
    sendJson(resp, code, error);
  }

  private String getFieldDescription(String field) {
    return "Field: " + field;
  }

  private String getFieldCategory(String f) {
    if (f.startsWith("sale.")) return "sale";
    if (f.startsWith("product.")) return "product";
    if (f.startsWith("customer.")) return "customer";
    if (f.startsWith("date.")) return "date";
    return "other";
  }

  private String getFieldExample(String f) {
    return switch (f) {
      case "sale.id" -> "SAL-001";
      case "sale.amount" -> "299.99";
      case "product.name" -> "Wireless Headphones";
      case "customer.name" -> "John Smith";
      default -> "example";
    };
  }

  private static class QueryResult {
    JsonNode data;
    ObjectNode metadata;
  }
}
