package com.gentoro.onemcp.openapi;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.acme.AcmeServer;
import com.gentoro.onemcp.http.EmbeddedJettyServer;
import com.gentoro.onemcp.utility.JacksonUtility;
import io.swagger.v3.oas.models.OpenAPI;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test to reproduce and diagnose the HTTP 405 error for querySalesData operation.
 * 
 * <p>This test:
 * 1. Sets up a real Jetty server with AcmeServer
 * 2. Tests URL construction from OpenAPI spec
 * 3. Tests actual HTTP POST request to /acme/query endpoint
 * 4. Verifies EndpointInvoker behavior
 * 
 * <p>To run this test:
 * <pre>{@code
 * mvn test -Dtest=QuerySalesDataHttp405Test
 * }</pre>
 * 
 * <p>The test will help identify:
 * - Whether the servlet is properly registered
 * - Whether the URL construction is correct
 * - Whether there's a mismatch between OpenAPI spec and actual server paths
 * - The exact URL being called when the 405 error occurs
 */
@DisplayName("QuerySalesData HTTP 405 Error Reproduction Test")
class QuerySalesDataHttp405Test {

  private OneMcp oneMcp;
  private EmbeddedJettyServer httpServer;
  private int serverPort;
  private String baseUrl;

  @BeforeEach
  void setUp() throws Exception {
    // Set up test environment
    Path testHomeDir = Paths.get("target", "test-reports", "http405-test");
    Files.createDirectories(testHomeDir);
    System.setProperty("ONEMCP_HOME_DIR", testHomeDir.toAbsolutePath().toString());

    // Set handbook path to Acme handbook
    Path acmeHandbookPath = Paths.get("src", "main", "resources", "acme-handbook");
    if (!Files.exists(acmeHandbookPath)) {
      throw new IllegalStateException(
          "Acme handbook not found at: " + acmeHandbookPath.toAbsolutePath());
    }
    System.setProperty("HANDBOOK_DIR", acmeHandbookPath.toAbsolutePath().toString());

    // Find an available port
    serverPort = findAvailablePort();
    baseUrl = "http://localhost:" + serverPort;

    // Create temporary config file
    Path tempConfigFile = testHomeDir.resolve("test-application.yaml");
    String configContent = String.format(
        "handbook:\n" +
        "  location: ${env:HANDBOOK_DIR:-classpath:acme-handbook}\n" +
        "\n" +
        "prompt:\n" +
        "  location: classpath:prompts\n" +
        "\n" +
        "http:\n" +
        "  port: %d\n" +
        "  acme:\n" +
        "    context-path: /acme\n" +
        "\n" +
        "llm:\n" +
        "  active-profile: test-llm\n" +
        "  test-llm:\n" +
        "    provider: openai\n" +
        "    apiKey: test-key\n" +
        "    model: test-model\n" +
        "\n" +
        "logging:\n" +
        "  dir: logs\n" +
        "  level:\n" +
        "    root: WARN\n" +
        "    com:\n" +
        "      gentoro:\n" +
        "        onemcp: DEBUG\n",
        serverPort);
    Files.writeString(tempConfigFile, configContent);

    // Initialize OneMcp with config file
    String configPath = tempConfigFile.toAbsolutePath().toUri().toString();
    String[] appArgs = new String[] {
        "--config-file", configPath,
        "--mode", "server"
    };
    oneMcp = new OneMcp(appArgs);
    oneMcp.initialize();

    // Get HTTP server (should already have AcmeServer registered via initialize())
    httpServer = oneMcp.httpServer();

    // Wait for server to be ready
    Thread.sleep(500);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
    if (oneMcp != null) {
      oneMcp.shutdown();
    }
  }

  @Test
  @Timeout(30)
  @DisplayName("Test 1: Verify servlet is registered and responds to POST")
  void testServletRegistration() throws Exception {
    // Test direct HTTP POST to /acme/query
    String url = baseUrl + "/acme/query";
    System.out.println("Testing URL: " + url);

    ObjectMapper mapper = JacksonUtility.getJsonMapper();
    com.fasterxml.jackson.databind.node.ObjectNode requestBody = mapper.createObjectNode();
    requestBody.putArray("fields").add("sale.amount");
    com.fasterxml.jackson.databind.node.ArrayNode aggregatesArray = requestBody.putArray("aggregates");
    com.fasterxml.jackson.databind.node.ObjectNode aggregate = aggregatesArray.addObject();
    aggregate.put("field", "sale.amount");
    aggregate.put("function", "sum");
    aggregate.put("alias", "total_sales");

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
        .build();

    HttpClient client = HttpClient.newHttpClient();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    System.out.println("Response status: " + response.statusCode());
    System.out.println("Response body: " + response.body());

    // Should not be 405
    assertNotEquals(405, response.statusCode(),
        "Servlet should accept POST requests. Got 405 - method not allowed. "
            + "Response: " + response.body());

    // Should be 200 or 400 (400 if request is invalid, but not 405)
    assertTrue(response.statusCode() == 200 || response.statusCode() == 400,
        "Expected 200 or 400, got " + response.statusCode() + ". Response: " + response.body());
  }

  @Test
  @Timeout(30)
  @DisplayName("Test 2: Test EndpointInvoker URL construction")
  void testEndpointInvokerUrlConstruction() throws Exception {
    // Load OpenAPI spec
    Path specPath = Paths.get("src", "main", "resources", "acme-handbook", "apis",
        "sales-analytics-api.yaml");
    OpenAPI openAPI = OpenApiLoader.load(specPath.toString());

    // Get the querySalesData operation
    io.swagger.v3.oas.models.PathItem pathItem = openAPI.getPaths().get("/query");
    assertNotNull(pathItem, "Path /query should exist in OpenAPI spec");

    io.swagger.v3.oas.models.Operation operation = pathItem.getPost();
    assertNotNull(operation, "POST operation should exist for /query");
    assertEquals("querySalesData", operation.getOperationId(),
        "Operation ID should be querySalesData");

    // Get server URL from OpenAPI spec
    String serverUrl = openAPI.getServers().get(0).getUrl();
    System.out.println("OpenAPI server URL: " + serverUrl);

    // Build invoker with actual server URL (replace port if needed)
    String actualBaseUrl = serverUrl.replace("8080", String.valueOf(serverPort));
    System.out.println("Actual base URL: " + actualBaseUrl);

    EndpointInvoker invoker = new EndpointInvoker(actualBaseUrl, "/query", "POST", operation);

    // Test URL construction with structured HTTP request
    ObjectMapper mapper = JacksonUtility.getJsonMapper();
    com.fasterxml.jackson.databind.node.ObjectNode bodyNode = mapper.createObjectNode();
    bodyNode.putArray("fields").add("sale.amount");
    com.fasterxml.jackson.databind.node.ArrayNode aggregatesArray = bodyNode.putArray("aggregates");
    com.fasterxml.jackson.databind.node.ObjectNode aggregate = aggregatesArray.addObject();
    aggregate.put("field", "sale.amount");
    aggregate.put("function", "sum");
    aggregate.put("alias", "total_sales");

    com.fasterxml.jackson.databind.node.ObjectNode httpRequest = mapper.createObjectNode();
    httpRequest.set("body", bodyNode);

    // The invoke method should construct: baseUrl + path = http://localhost:PORT/acme + /query
    // Expected: http://localhost:PORT/acme/query
    try {
      JsonNode result = invoker.invoke(httpRequest);
      System.out.println("Success! Result: " + mapper.writerWithDefaultPrettyPrinter()
          .writeValueAsString(result));
      assertNotNull(result, "Result should not be null");
    } catch (RuntimeException e) {
      String errorMsg = e.getMessage();
      System.out.println("Error message: " + errorMsg);

      // Check if it's a 405 error
      if (errorMsg != null && errorMsg.contains("405")) {
        fail("HTTP 405 error occurred. This indicates the URL is wrong or servlet not registered. "
            + "Error: " + errorMsg);
      }
      throw e;
    }
  }

  @Test
  @Timeout(30)
  @DisplayName("Test 3: Test URL construction edge cases")
  void testUrlConstructionEdgeCases() throws Exception {
    // Test various baseUrl formats
    String[] baseUrls = {
        "http://localhost:" + serverPort + "/acme",      // With trailing slash in path
        "http://localhost:" + serverPort + "/acme/",     // With trailing slash
        "http://localhost:" + serverPort + "/acme",      // Without trailing slash
    };

    String path = "/query";

    for (String baseUrl : baseUrls) {
      String constructedUrl = baseUrl + path;
      System.out.println("Base URL: " + baseUrl);
      System.out.println("Constructed URL: " + constructedUrl);

      // Test if URL is accessible
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(constructedUrl))
          .header("Content-Type", "application/json")
          .method("POST", HttpRequest.BodyPublishers.ofString("{}"))
          .build();

      HttpClient client = HttpClient.newHttpClient();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      System.out.println("  Status: " + response.statusCode());
      if (response.statusCode() == 405) {
        System.out.println("  ❌ 405 Error - URL construction issue detected!");
        System.out.println("  Response: " + response.body());
      } else {
        System.out.println("  ✅ URL works (status: " + response.statusCode() + ")");
      }
    }
  }

  @Test
  @Timeout(30)
  @DisplayName("Test 4: Verify OpenAPI spec server URL matches actual server")
  void testOpenApiServerUrlMatch() throws Exception {
    Path specPath = Paths.get("src", "main", "resources", "acme-handbook", "apis",
        "sales-analytics-api.yaml");
    OpenAPI openAPI = OpenApiLoader.load(specPath.toString());

    String specServerUrl = openAPI.getServers().get(0).getUrl();
    System.out.println("OpenAPI spec server URL: " + specServerUrl);

    // Extract path from server URL
    URI specUri = URI.create(specServerUrl);
    String specPathFromUrl = specUri.getPath(); // Should be "/acme"
    System.out.println("Path from spec URL: " + specPathFromUrl);

    // Verify servlet is registered at correct path
    String expectedServletPath = "/acme/query";
    System.out.println("Expected servlet path: " + expectedServletPath);

    // Test the full URL
    String fullUrl = "http://localhost:" + serverPort + expectedServletPath;
    System.out.println("Full URL to test: " + fullUrl);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(fullUrl))
        .header("Content-Type", "application/json")
        .method("POST", HttpRequest.BodyPublishers.ofString("{\"fields\":[\"sale.amount\"]}"))
        .build();

    HttpClient client = HttpClient.newHttpClient();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    System.out.println("Response status: " + response.statusCode());
    System.out.println("Response body: " + response.body());

    if (response.statusCode() == 405) {
      fail("HTTP 405 error at " + fullUrl + ". This suggests a URL mismatch. "
          + "Spec URL: " + specServerUrl + ", Actual server path: " + expectedServletPath);
    }
  }

  /**
   * Find an available port for testing.
   */
  private int findAvailablePort() {
    try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (Exception e) {
      throw new RuntimeException("Could not find available port", e);
    }
  }
}

