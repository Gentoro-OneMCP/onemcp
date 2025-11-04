package com.gentorox.protocols;

import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.agent.Orchestrator;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.WebFluxStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit-level tests for McpServerConfig (HTTP Streamable MCP over Spring WebFlux).
 *
 * What this covers:
 * - transport provider and router beans are created in the context;
 * - the async tool is registered with the expected name and description;
 * - the router denies DELETE requests when disallowDelete is true (basic transport behavior).
 *
 * Note: This class focuses on configuration wiring and bean exposure. A separate integration test
 * (McpHttpStreamableClientIntegrationTest) exercises the protocol with the official MCP Java SDK client.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = McpServerConfigTest.TestApp.class, properties = {
    "agentProtocol.mcp.config.messageEndpoint=/mcp",
    "agentProtocol.mcp.config.disallowDelete=true",
    // ensure we don't need real external services during context startup
    "spring.main.web-application-type=reactive"
})
class McpServerConfigTest {

  @org.springframework.boot.SpringBootConfiguration
  @org.springframework.boot.autoconfigure.EnableAutoConfiguration
  @org.springframework.context.annotation.Import(McpServerConfig.class)
  static class TestApp {}

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private WebFluxStreamableServerTransportProvider transportProvider;

  @Autowired
  private RouterFunction<?> mcpRouterFunction;

  @Autowired
  private McpServerFeatures.AsyncToolSpecification asyncToolSpecification;

  @Value("${agentProtocol.mcp.config.messageEndpoint:/mcp}")
  private String messageEndpoint;

  @MockBean
  private Orchestrator orchestrator; // mocked to allow asyncToolSpecification bean creation

  /**
   * Stubs Orchestrator to avoid hitting real providers and make tool invocation deterministic.
   */
  @BeforeEach
  void setUp() {
    // Stub orchestrator.run to return a deterministic response if invoked by any test utility
    when(orchestrator.run(any(), any())).thenReturn(new InferenceResponse(
        "ok", Optional.empty(), "test-trace"));
  }

  /**
   * Validates the Spring context starts and essential beans from McpServerConfig are present.
   */
  @Test
  void contextLoads_andBeansPresent() {
    assertThat(applicationContext).isNotNull();

    // Beans from McpServerConfig must be present
    assertThat(transportProvider).isNotNull();
    assertThat(mcpRouterFunction).isNotNull();
    assertThat(asyncToolSpecification).isNotNull();
  }

  @Test
  void asyncTool_isRegisteredWithExpectedName() {
    // The tool spec should expose the tool definition; verify name
    McpSchema.Tool tool = asyncToolSpecification.tool();
    assertThat(tool).isNotNull();
    assertThat(tool.name()).isEqualTo("gentoro.run");
    assertThat(tool.description()).contains("Gentoro Agent");
  }

  @Test
  void router_disallowsDeleteRequests_whenConfigured() {
    // Bind a WebTestClient to the router and issue a DELETE to the endpoint; we only assert a 4xx (typically 405)
    WebTestClient client = WebTestClient.bindToRouterFunction(mcpRouterFunction).configureClient().build();

    client.delete().uri(messageEndpoint)
        .exchange()
        .expectStatus().is4xxClientError();
  }
}
