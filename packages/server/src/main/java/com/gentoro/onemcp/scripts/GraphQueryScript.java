package com.gentoro.onemcp.scripts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.indexing.GraphQueryService;
import com.gentoro.onemcp.logging.LoggingService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Small utility entry point that wires {@link GraphQueryService} with a {@link OneMcp} context to
 * execute the ACME handbook graph query locally.
 *
 * <p>Usage:
 *
 * <pre>
 *   mvn -q -pl packages/server -am package
 *   java -cp packages/server/target/onemcp-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *        com.gentoro.onemcp.scripts.GraphQueryScript \
 *        --config-file classpath:application.yaml \
 *        --context-file /path/to/context.json
 * </pre>
 *
 * The script defaults to the ACME handbook context provided in the README when no custom context
 * file is supplied.
 */
public final class GraphQueryScript {
  private static final org.slf4j.Logger log = LoggingService.getLogger(GraphQueryScript.class);

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private GraphQueryScript() {}

  public static void main(String[] args) {
    CliArgs cliArgs = CliArgs.parse(args);

    GraphQueryService.QueryRequest request;
    try {
      request = loadRequest(cliArgs.contextFile());
    } catch (IOException e) {
      log.error("Unable to load query context", e);
      System.exit(1);
      return;
    }

    String[] startupArgs = buildStartupArgs(cliArgs);
    OneMcp oneMcp = new OneMcp(startupArgs);

    try {
      log.info("Initializing OneMCP context using config: {}", cliArgs.configFile());
      oneMcp.initialize();

      try (GraphQueryService graphQueryService = new GraphQueryService(oneMcp)) {
        log.info("Running graph query for {} context items", request.getContext().size());
        List<GraphQueryService.QueryResult> results = graphQueryService.query(request);
        printResults(results);
      }
    } catch (Exception e) {
      log.error("Graph query execution failed", e);
      System.exit(2);
    } finally {
      log.info("Shutting down OneMCP context");
      oneMcp.shutdown();
    }
  }

  private static GraphQueryService.QueryRequest loadRequest(String contextFile) throws IOException {
    if (contextFile == null || contextFile.isBlank()) {
      return defaultRequest();
    }

    log.info("Loading query context from file: {}", contextFile);
    try {
      String json = Files.readString(Path.of(contextFile));
      return MAPPER.readValue(json, GraphQueryService.QueryRequest.class);
    } catch (IOException e) {
      throw new IOException("Failed to read query context from " + contextFile, e);
    }
  }

  private static GraphQueryService.QueryRequest defaultRequest() {
    List<GraphQueryService.ContextItem> contextItems = new ArrayList<>();

    contextItems.add(
        new GraphQueryService.ContextItem("Sale", List.of("Retrieve", "Compute"), 100, "direct"));
    contextItems.add(
        new GraphQueryService.ContextItem("Customer", List.of("Retrieve"), 100, "direct"));
    contextItems.add(
        new GraphQueryService.ContextItem("Product", List.of("Retrieve"), 100, "indirect"));

    return new GraphQueryService.QueryRequest(contextItems);
  }

  private static void printResults(List<GraphQueryService.QueryResult> results)
      throws JsonProcessingException {
    Map<String, Object> payload = new HashMap<>();
    payload.put("results", results);
    payload.put("count", results.size());

    String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
    System.out.println(json);
  }

  private static String[] buildStartupArgs(CliArgs cliArgs) {
    List<String> params = new ArrayList<>();
    params.add("--mode");
    params.add(cliArgs.mode());
    params.add("--config-file");
    params.add(cliArgs.configFile());
    return params.toArray(String[]::new);
  }

  private record CliArgs(String configFile, String contextFile, String mode) {
    static CliArgs parse(String[] args) {
      Map<String, String> values = new HashMap<>();
      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (!arg.startsWith("--")) {
          continue;
        }
        String key = arg.substring(2).toLowerCase(Locale.ROOT);
        String value = (i + 1) < args.length ? args[i + 1] : null;
        if (value != null && value.startsWith("--")) {
          value = null;
        } else if (value != null) {
          i++;
        }
        values.put(key, value);
      }

      String config = values.getOrDefault("config-file", "classpath:application.yaml");
      String context = values.get("context-file");
      String mode = values.getOrDefault("mode", "server");

      if (!List.of("interactive", "dry-run", "server").contains(mode)) {
        log.warn("Unsupported mode '{}', defaulting to 'server'", mode);
        mode = "server";
      }

      return new CliArgs(config, context, mode);
    }
  }
}
