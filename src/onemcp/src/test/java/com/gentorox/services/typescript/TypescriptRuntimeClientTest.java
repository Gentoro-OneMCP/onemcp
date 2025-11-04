package com.gentorox.services.typescript;

import kotlin.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TypescriptRuntimeClient} mocking the underlying WebClient.
 */
public class TypescriptRuntimeClientTest {

  private TypescriptRuntimeClient client;
  private WebClient webClient;

  @BeforeEach
  void setUp() throws Exception {
    client = new TypescriptRuntimeClient("http://localhost");
    webClient = Mockito.mock(WebClient.class, Mockito.RETURNS_DEEP_STUBS);

    // Inject mocked WebClient into the client via reflection since constructor creates it internally
    Field web = TypescriptRuntimeClient.class.getDeclaredField("web");
    web.setAccessible(true);
    web.set(client, webClient);
  }

  @Test
  void exec_shouldReturnRunResponse_onSuccess() {
    // Arrange using deep stubs instead of manual chain types
    WebClient webDeep = Mockito.mock(WebClient.class, Mockito.RETURNS_DEEP_STUBS);
    // Inject deep mock
    try {
      Field web = TypescriptRuntimeClient.class.getDeclaredField("web");
      web.setAccessible(true);
      web.set(client, webDeep);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    TypescriptRuntimeClient.RunResponse expected = new TypescriptRuntimeClient.RunResponse(true, null, List.of(new TypescriptRuntimeClient.LogEntry("log", List.of("hello"))), null);
    when(webDeep.post()
        .uri(eq("/run"))
        .contentType(eq(MediaType.APPLICATION_JSON))
        .bodyValue(any())
        .retrieve()
        .onStatus(any(), any())
        .bodyToMono(eq(String.class))
        .onErrorResume(any())
        .doOnNext(ArgumentMatchers.<Consumer<String>>any())
        .flatMap(any())
    ).thenReturn(Mono.just(expected));

    // Act
    TypescriptRuntimeClient.RunResponse out = client.exec("console.log('hello');").block();

    // Assert
    assertThat(out).isNotNull();
    assertThat(out.ok()).isTrue();
    assertThat(out.logs()).isNotNull();
    assertThat(out.logs()).isNotEmpty();
    assertThat(out.logs().stream().anyMatch( e -> e.args().stream().anyMatch( v -> v.contains("hello") ) )).isTrue();
  }

  @Test
  void uploadOpenapi_shouldPostMultipart_andReturnResult() throws Exception {
    // Arrange: create a temp file to pass as spec (it won't actually be read because of mocking)
    Path tmpSpec = Files.createTempFile("spec", ".yaml");

    TypescriptRuntimeClient.UploadResult expected = new TypescriptRuntimeClient.UploadResult(true, new TypescriptRuntimeClient.Sdk("petstore", "/tmp/external-sdks/petstore", "file:///tmp/external-sdks/petstore"), "created");
    when(webClient.post()
        .uri(eq("/sdk/upload"))
        .contentType(eq(MediaType.MULTIPART_FORM_DATA))
        .body(any())
        .retrieve()
        .bodyToMono(eq(TypescriptRuntimeClient.UploadResult.class)))
        .thenReturn(Mono.just(expected));

    // Act
    TypescriptRuntimeClient.UploadResult out = client.uploadOpenapi(tmpSpec, "petstore", false).block();

    // Assert
    assertThat(out).isNotNull();
    assertThat(out.ok()).isTrue();
    assertThat(out.sdk().entry()).contains("petstore");
  }
}
