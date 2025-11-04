package com.gentorox.services.typescript;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentorox.tools.RetrieveContextTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * HTTP client for the external TypeScript Runtime service.
 *
 * Features:
 * - Generate a TypeScript SDK from an OpenAPI specification by uploading a file.
 * - Execute short TypeScript code snippets using previously generated SDKs.
 *
 *
 * This class is stateless and thread-safe.
 */
@Component
public class TypescriptRuntimeClient {
  private static final Logger logger = LoggerFactory.getLogger(TypescriptRuntimeClient.class);
  private final WebClient web;

  /**
   * Creates a new TypescriptRuntimeClient.
   *
   * @param baseUrl the base URL of the runtime service, e.g. http://localhost:7070
   */
  public TypescriptRuntimeClient(@Value("${typescriptRuntime.baseUrl:http://localhost:7070}") String baseUrl) {
    this.web = WebClient.builder()
        .baseUrl(baseUrl)
        .build();
  }

  /**
   * Execute a short piece of TypeScript code on the runtime service.
   *
   * Sends a POST request to /run with JSON body: { "snippet": "<code>" }.
   * The response is deserialized into {@link RunResponse}.
   *
   * @param code TypeScript code to execute (non-null)
   * @return a reactive Mono emitting the {@link RunResponse} returned by the runtime
   */
  public Mono<RunResponse> exec(String code) {
    logger.info("Executing snippet: {}", code);
    return web.post()
        .uri("/run")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("snippet", code))
        .retrieve()
        .onStatus(s -> !s.is2xxSuccessful(),
            resp -> resp.bodyToMono(String.class).map(body ->
                new IllegalStateException("Docs request failed: " + resp.statusCode() + " - " + body)))
        .bodyToMono(String.class)                 // read raw string
        .onErrorResume(e -> {
          StringWriter sw = new StringWriter(4096);
          try (PrintWriter pw = new PrintWriter(sw)) {
            e.printStackTrace(pw);
          }
          try {
            return Mono.just(new ObjectMapper().writeValueAsString(new TypescriptRuntimeClient.RunResponse(
                false, null, Collections.emptyList(), sw.toString())));
          } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
          }
        })
        .doOnNext(body -> logger.info("Raw response: {}", body)) // inspect/log it
        .flatMap(body -> {
          try {
            RunResponse parsed = new ObjectMapper().readValue(body, RunResponse.class);
            return Mono.just(parsed);
          } catch (Exception e) {
            logger.error("Failed to parse response", e);
            return Mono.error(e);
          }
        });
  }

  /**
   * Generate a TypeScript SDK by uploading a local OpenAPI file.
   *
   * Sends multipart/form-data to POST /sdk/upload with fields:
   * - spec: the OpenAPI YAML/JSON file (binary)
   * - outDir: desired output directory name on the runtime side
   *
   * @param specPath path to the local OpenAPI YAML/JSON file
   * @param outDir   desired output directory name on the runtime side
   * @param cleanup   flag to force typescript runtime cleaning its cache.
   * @return a reactive Mono emitting the {@link UploadResult} returned by the runtime
   */
  public Mono<UploadResult> uploadOpenapi(Path specPath, String outDir, boolean cleanup) {
    MultipartBodyBuilder mb1 = new MultipartBodyBuilder();
    mb1.part("spec", new FileSystemResource(specPath.toFile()));
    mb1.part("outDir", outDir);
    mb1.part("cleanup", String.valueOf(cleanup));

    return web.post()
        .uri("/sdk/upload")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(mb1.build()))
        .retrieve()
        .bodyToMono(UploadResult.class)
        .retry(3) // Retry up to 3 times on failure
        .doOnError(e -> logger.warn("Failed to upload OpenAPI spec after retries: {}", e.getMessage()));
  }

  /**
   * Fetches the Markdown documentation collection for a generated SDK namespace.
   *
   * <p>This method calls the server endpoint
   * <code>GET /sdk/docs/{namespace}?cleanup={cleanup}</code>
   * and returns the parsed {@link DocsResponse} emitted as a {@link reactor.core.publisher.Mono}.
   * The endpoint triggers documentation generation (if not already present)
   * and returns a structured list of Markdown files representing the SDK reference.</p>
   *
   * <p>Usage example:</p>
   *
   * <pre>{@code
   * docsService.fetchDocs("calendry", false)
   *     .doOnNext(resp -> System.out.println("Files: " + resp.count()))
   *     .flatMapMany(resp -> Flux.fromIterable(resp.files()))
   *     .subscribe(file -> System.out.println(file.path()));
   * }</pre>
   *
   * <h3>Behavior</h3>
   * <ul>
   *   <li>Performs an HTTP GET request using {@link WebClient}.</li>
   *   <li>Sets the <code>cleanup</code> query parameter — when true, the server will remove
   *       any previously generated docs before regenerating them.</li>
   *   <li>Accepts <code>application/json</code> and expects the response body to conform
   *       to {@link DocsResponse}.</li>
   *   <li>Non-2xx responses are mapped to an {@link IllegalStateException}
   *       containing the HTTP status and response body.</li>
   *   <li>The call automatically times out after 30 seconds.</li>
   * </ul>
   *
   * <h3>Parameters</h3>
   * <ul>
   *   <li><b>namespace</b> — The SDK namespace (folder name) whose docs should be retrieved.
   *       This corresponds to the directory name generated during SDK upload.</li>
   *   <li><b>cleanup</b> — Whether to force a regeneration of the Markdown docs
   *       (<code>true</code>) or use existing cached output (<code>false</code>).</li>
   * </ul>
   *
   * <h3>Returns</h3>
   * <p>A {@link Mono} emitting one {@link DocsResponse} object on success,
   * or an error signal if the request fails or times out.</p>
   *
   * <h3>Errors</h3>
   * <ul>
   *   <li>{@link IllegalStateException} — when the server responds with a non-2xx status.</li>
   *   <li>{@link java.util.concurrent.TimeoutException} — if the request exceeds 30 seconds.</li>
   * </ul>
   *
   * @param namespace the SDK namespace to fetch documentation for
   * @param cleanup whether to force regeneration of the docs before returning
   * @return a {@link Mono} emitting the resulting {@link DocsResponse}
   */
  public Mono<DocsResponse> fetchDocs(String namespace, boolean cleanup) {
    return web.get()
        .uri(uri -> uri
            .path("/sdk/docs/{ns}")
            .queryParam("cleanup", cleanup)
            .build(namespace))
        .accept(MediaType.APPLICATION_JSON)
        .retrieve()
        .onStatus(s -> !s.is2xxSuccessful(),
        resp -> resp.bodyToMono(String.class).map(body ->
            new IllegalStateException("Docs request failed: " + resp.statusCode() + " - " + body)))
        .bodyToMono(DocsResponse.class)
        .timeout(java.time.Duration.ofSeconds(30))
        .retry(3) // Retry up to 3 times on failure
        .doOnError(e -> logger.warn("Failed to fetch docs after retries: {}", e.getMessage()));
  }

  /**
   * Represents the JSON response shape returned by the `/run` endpoint.
   *
   * <p>This endpoint executes a code snippet and returns structured logs,
   * potential output values, and error information. A typical response
   * might look like:
   *
   * <pre>{@code
   * {
   *   "ok": true,
   *   "value": null,
   *   "logs": [
   *     { "level": "log", "args": ["hi"] }
   *   ],
   *   "error": null
   * }
   * }</pre>
   *
   * @param ok whether the execution completed successfully
   * @param value the returned value from the executed snippet, or {@code null} if none
   * @param logs a list of console log entries (stdout / stderr) emitted during execution
   * @param error an error message if execution failed, or {@code null} otherwise
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record RunResponse(
      boolean ok,
      Object value,
      List<LogEntry> logs,
      String error
  ) {}

  /**
   * Represents a single console log entry from a `/run` response.
   *
   * <p>Each log entry corresponds to one console output event and includes
   * the log level (e.g. "log", "warn", "error") and the arguments that were
   * printed in that call.
   *
   * <pre>{@code
   * {
   *   "level": "log",
   *   "args": ["hi"]
   * }
   * }</pre>
   *
   * @param level the console log level (e.g. "log", "warn", "error")
   * @param args the raw arguments passed to the console function
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record LogEntry(
      String level,
      List<String> args
  ) {}

  /**
   * Represents the JSON response shape returned by the `/sdk/upload` endpoint.
   *
   * <p>This endpoint uploads and registers an external SDK. Upon success,
   * it returns metadata about the generated SDK and a user-facing message.
   *
   * <pre>{@code
   * {
   *   "ok": true,
   *   "sdk": {
   *     "namespace": "simpleapi_5",
   *     "location": "/tmp/external-sdks/simpleapi_5",
   *     "entry": "file:///tmp/external-sdks/simpleapi_5/index.ts"
   *   },
   *   "message": "SDK generated and will be auto-loaded on /run under sdk.<namespace>"
   * }
   * }</pre>
   *
   * @param ok whether the SDK upload and generation completed successfully
   * @param sdk metadata describing the generated SDK (namespace, location, entry point)
   * @param message human-readable message describing the outcome
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record UploadResult(
      boolean ok,
      Sdk sdk,
      String message
  ) {}

  /**
   * Metadata describing a generated SDK artifact from the `/sdk/upload` response.
   *
   * <p>This object contains the internal namespace assigned to the SDK, the
   * filesystem location where it was stored, and the URI of its entry point file.
   *
   * <pre>{@code
   * {
   *   "namespace": "simpleapi_5",
   *   "location": "/tmp/external-sdks/simpleapi_5",
   *   "entry": "file:///tmp/external-sdks/simpleapi_5/index.ts"
   * }
   * }</pre>
   *
   * @param namespace the unique SDK namespace (used as identifier under {@code sdk.<namespace>})
   * @param location the absolute local directory path where the SDK is stored
   * @param entry the URI to the main entry file of the SDK
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Sdk(
      String namespace,
      String location,
      String entry
  ) {}


  /**
   * Represents a single Markdown documentation file returned by the SDK docs endpoint.
   *
   * <p>Each {@code DocFile} corresponds to one generated .md file, such as
   * "ActivityLogApi.md" or "DefaultApi.md".</p>
   *
   * <ul>
   *   <li><b>path</b> — relative path of the file inside the docs directory.</li>
   *   <li><b>markdown</b> — full Markdown content of the file.</li>
   * </ul>
   *
   * Example JSON:
   * <pre>{@code
   * {
   *   "path": "ActivityLogApi.md",
   *   "markdown": "# ActivityLogApi\n\nAll URIs are relative to *https://api.calendly.com*\n..."
   * }
   * }</pre>
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record DocFile(String path, String markdown) {}

  /**
   * Represents the JSON response returned by {@code GET /sdk/docs/{namespace}}.
   *
   * <p>The server builds Markdown documentation from a generated SDK or OpenAPI spec
   * and returns a structured collection of files. The response may also include
   * information about where the files were written on disk.</p>
   *
   * <ul>
   *   <li><b>ok</b> — {@code true} if the documentation was successfully generated.</li>
   *   <li><b>namespace</b> — logical namespace (SDK folder name) for which the docs were created.</li>
   *   <li><b>count</b> — total number of Markdown files included.</li>
   *   <li><b>files</b> — list of {@link DocFile} entries, each containing path + Markdown text.</li>
   *   <li><b>diskLocation</b> — absolute filesystem path of the generated docs (optional).</li>
   *   <li><b>message</b> — human-readable status or description (optional).</li>
   * </ul>
   *
   * Example JSON:
   * <pre>{@code
   * {
   *   "ok": true,
   *   "namespace": "calendry",
   *   "count": 194,
   *   "files": [
   *     {
   *       "path": "ActivityLogApi.md",
   *       "markdown": "# ActivityLogApi\\n..."
   *     }
   *   ],
   *   "diskLocation": "/tmp/external-sdks/calendry/docs.openapi-generator.markdown",
   *   "message": "Documentation generated via OpenAPI Generator (markdown)."
   * }
   * }</pre>
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record DocsResponse(
      boolean ok,
      String namespace,
      int count,
      List<DocFile> files,
      String diskLocation,
      String message
  ) {}
}
