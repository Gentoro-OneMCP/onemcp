package com.gentoro.onemcp.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import java.util.Collections;
import java.util.Objects;

public class Example {
  public static void main(String[] args) throws Exception {
    OpenAPI specDownloads =
        OpenApiLoader.load(
            "/Users/altamirosantos/IdeaProjects/mcpagent-product/src/acme-analytics-server/mcpagent-handbook/openapi/sales-analytics-api.yaml"); // or "openapi.json"
    String serverUrl =
        Objects.requireNonNullElse(specDownloads.getServers(), Collections.<Server>emptyList())
            .stream()
            .map(Server::getUrl)
            .findFirst()
            .orElse("https://api.example.com");
    OpenApiProxy proxyDownloads = new OpenApiProxyImpl(specDownloads, serverUrl);
    // System.out.println( String.join(("\n\n---\n\n"), MarkdownGenerator.generate(specDownloads,
    // serverUrl)));

    System.out.println(
        """
        %s

        %s

        ---\n\n
        """
            .formatted(
                StringUtility.sanitize(specDownloads.getInfo().getTitle()).toLowerCase(),
                StringUtility.formatWithIndent(specDownloads.getInfo().getDescription(), 2)));

    // specDownloads.getInfo().getDescription();

    // 1️⃣ Load OpenAPI specification
    OpenAPI spec = OpenApiLoader.load("openapi.yaml"); // or "openapi.json"

    // 2️⃣ Create dynamic proxy
    OpenApiProxy proxy = new OpenApiProxyImpl(spec, "https://api.example.com");

    // 3️⃣ Example JSON input matching an endpoint definition
    String jsonInput =
        """
        {
          "userId": "U001",
          "lang": "en",
          "X-Request-ID": "req-999",
          "sessionToken": "abc123",
          "body": {
            "productId": "P100",
            "quantity": 2,
            "note": "Urgent order"
          }
        }
        """;

    JsonNode input = JacksonUtility.getJsonMapper().readTree(jsonInput);

    // 4️⃣ Invoke dynamically using operationId from OpenAPI
    JsonNode response = proxy.invoke("createOrder", input);

    // 5️⃣ Print result
    System.out.println(response.toPrettyString());
  }
}
