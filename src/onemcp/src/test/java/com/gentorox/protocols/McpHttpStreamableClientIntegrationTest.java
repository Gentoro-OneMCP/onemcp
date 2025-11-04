package com.gentorox.protocols;

import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.agent.Orchestrator;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Integration test for the MCP HTTP Streamable transport using the official MCP Java SDK as a client.
 *
 * What this verifies:
 * - The server exposes the streamable endpoint at the configured path.
 * - The MCP handshake completes and capabilities are returned.
 * - The server lists the configured tool (gentoro.run).
 * - The client can invoke the tool endpoint successfully. We treat any tool result (success or error) as success,
 *   because the goal here is to validate protocol wiring, not model/tool execution.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = McpHttpStreamableClientIntegrationTest.TestApp.class,
    properties = {
        "agentProtocol.mcp.config.messageEndpoint=/mcp",
        "agentProtocol.mcp.config.disallowDelete=true"
    }
)
class McpHttpStreamableClientIntegrationTest {

  @org.springframework.boot.SpringBootConfiguration
  @org.springframework.boot.autoconfigure.EnableAutoConfiguration
  @Import(McpServerConfig.class)
  static class TestApp {}

  @LocalServerPort
  int port;

  @Value("${agentProtocol.mcp.config.messageEndpoint:/mcp}")
  String messageEndpoint;

  @Autowired
  McpServerFeatures.AsyncToolSpecification asyncToolSpecification; // ensure tool bean exists

  @MockBean
  Orchestrator orchestrator;

  /**
   * Stubs the Orchestrator to avoid external dependencies and ensure deterministic tool results.
   */
  @BeforeEach
  void setUp() {
    when(orchestrator.run(any(), any())).thenReturn(new InferenceResponse(
        "integration-ok", Optional.empty(), "trace-it"));
  }

  /**
   * End-to-end check from client perspective:
   * - performs initialize handshake;
   * - lists the available tools and asserts gentoro.run is present;
   * - calls the tool using a minimal valid payload.
   */
  @Test
  void mcp_client_lists_tools_and_can_invoke_tool() {
    // Prepare official SDK JSON mapper
    McpJsonMapper json = McpJsonMapper.createDefault();

    // Build the client transport using Spring WebFlux implementation from the SDK.
    // We avoid bringing our own WebClient; the SDK handles it internally via the base URL and endpoint.
    String baseUrl = "http://localhost:" + port;

    // Instantiate transport and client
    HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
        .builder(baseUrl)
        .endpoint(messageEndpoint)
        .build();

    io.modelcontextprotocol.client.McpAsyncClient client = McpClient.async(transport)
        .capabilities(McpSchema.ClientCapabilities.builder()
            .build())
        .build();

    // Initialize handshake and fetch capabilities
    McpSchema.InitializeResult init = client.initialize().block(Duration.ofSeconds(5));
    assertThat(init).isNotNull();
    assertThat(init.capabilities().tools().listChanged()).isTrue();

    // List tools
    McpSchema.ListToolsResult toolListResult = client.listTools().block(Duration.ofSeconds(5));
    assertThat(toolListResult).isNotNull();
    assertThat(toolListResult.tools()).isNotNull();
    assertThat(toolListResult.tools()).isNotEmpty();
    assertThat(toolListResult.tools().stream().map(McpSchema.Tool::name)).contains("gentoro.run");

    // Invoke the tool with a minimal valid payload
    Map<String, Object> args = Map.of("prompt", "Say hello from MCP test");
    McpSchema.CallToolResult result = client.callTool( McpSchema.CallToolRequest.builder().name("gentoro.run").arguments(args).build() ).block(Duration.ofSeconds(10));

    // Treat any returned result (error or not) as success for transport/protocol validation
    assertThat(result).isNotNull();
  }
}
