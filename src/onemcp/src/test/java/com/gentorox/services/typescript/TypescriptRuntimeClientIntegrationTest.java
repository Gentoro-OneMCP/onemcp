package com.gentorox.services.typescript;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests against a live TypeScript Runtime server.
 */
public class TypescriptRuntimeClientIntegrationTest {

  private final String typescriptRuntimeBaseUrl = System.getenv().getOrDefault("TS_RUNTIME_URL", "http://localhost:7070");

  @Test
  void exec_againstLiveServer_shouldReturnOutput() {
    TypescriptRuntimeClient client = new TypescriptRuntimeClient(typescriptRuntimeBaseUrl);
    // Minimal snippet compatible with /run API (no export default required)
    var result = client.exec("console.log('hi'); return 40 + 2").block();
    assertThat(result).isNotNull();
    assertThat(result.ok()).isTrue();
    assertThat(result.logs()).isNotNull();
    assertThat(result.logs()).isNotEmpty();
    assertThat(result.logs().stream().anyMatch( e -> e.args().stream().anyMatch( v -> v.contains("hi") ) )).isTrue();
    assertThat(result.value()).isEqualTo("42");
  }

  @Test
  void uploadOpenapi_againstLiveServer_shouldGenerateSdk() throws Exception {
    TypescriptRuntimeClient client = new TypescriptRuntimeClient(typescriptRuntimeBaseUrl);

    // Create a minimal OpenAPI 3.0 spec in a temporary file
    String openapi = """
openapi: 3.0.0
info:
  title: Simple API
  version: 1.0.0
paths:
  /ping:
    get:
      responses:
        '200':
          description: pong
""";
    Path tmp = Files.createTempFile("openapi-upload-", ".yaml");
    Files.writeString(tmp, openapi);

    // Attempt upload
    var res = client.uploadOpenapi(tmp, "simpleapi", true).block();

    // Basic assertions (shape may vary by server); this verifies the call and mapping work
    assertThat(res).isNotNull();
    assertThat(res.ok()).isTrue();
    assertThat(res.sdk().namespace()).contains("simpleapi");
    assertThat(res.sdk().entry()).isNotNull();
  }
}
