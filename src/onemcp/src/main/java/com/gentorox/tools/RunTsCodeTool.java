package com.gentorox.tools;

import com.gentorox.services.telemetry.TelemetryService;
import com.gentorox.services.typescript.TypescriptRuntimeClient;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.agent.tool.P;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.scheduler.Schedulers;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class RunTsCodeTool implements AgentTool {
  private final TypescriptRuntimeClient ts;
  private final TelemetryService telemetry;
  private static final Logger logger = LoggerFactory.getLogger(RunTsCodeTool.class);

  // One shared pool for bridging; daemon threads so they don't block shutdown.
  private static final ExecutorService TOOL_EXEC =
      new ThreadPoolExecutor(
          0, Math.max(4, Runtime.getRuntime().availableProcessors()),
          60L, TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          r -> {
            Thread t = new Thread(r, "run-ts-bridge");
            t.setDaemon(true);
            return t;
          });

  // Reasonable upper bound so a stuck script doesn't pin a request forever.
  private static final long TIMEOUT_SECONDS = 60;

  public RunTsCodeTool(TypescriptRuntimeClient ts, TelemetryService telemetry) {
    this.ts = ts; this.telemetry = telemetry;
  }


  @Tool(name="RunTypescriptSnippet", value = "Execute a short TypeScript snippet in the isolated runtime and return stdout/result")
  public String runTsCode(@P("TypeScript code to execute") String code) {
    Callable<String> task = () -> telemetry.inSpan("tool.execute", Map.of("tool", "runTsCode"), () -> {
      TypescriptRuntimeClient.RunResponse r = ts.exec(code).block(); // runs on a worker if we're bridging (safe), or on a non-reactor thread

      if (r == null ) {
        return "Snippet execution completed and produced not output.";
      }

      if( r.error() != null && !r.error().isBlank()) {
        return """
              Snippet execution failed, see the detail below:
              
              ```
              %s
              ```
              """.formatted(r.error());
      }

      if( r.logs() != null && !r.logs().isEmpty() ) {
        if( r.logs().stream().anyMatch( l -> l.level().equals("error")) ) {
          return
              """
              Snippet execution failed, see the detail below:
              
              ```
              %s
              ```
              """.formatted(r.logs().stream().filter( l -> l.level().equals("error") )
                  .map(l -> String.join("\n", l.args()))
                  .collect(Collectors.joining("\n")));
        }
      }

      if (r.value() == null) {
        return "Snippet execution completed with status {%s}, and produced not output.".formatted(r.ok() ? "OK" : "ERROR");
      }

      return (String) r.value();
    });

    try {
      // If we're on a Reactor non-blocking thread (e.g., reactor-http-nio-*), offload first.
      String output;
      if (Schedulers.isInNonBlockingThread()) {
        output = TOOL_EXEC.submit(task).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } else {
        output = task.call();
      }
      logger.info("RunTypescriptSnippet output: {}", output);
      // Otherwise it's safe to run synchronously.
      return output;
    } catch (TimeoutException te) {
      logger.error("Timeout executing TypeScript snippet", te);
      return "(timeout after " + TIMEOUT_SECONDS + "s)";
    } catch (ExecutionException ee) {
      logger.error("Error executing TypeScript snippet", ee);
      // unwrap and show cause message minimally (or rethrow if you prefer)
      Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
      return "(error: " + cause.getClass().getSimpleName() + (cause.getMessage() != null ? (": " + cause.getMessage()) : "") + ")";
    } catch (InterruptedException ie) {
      logger.error("Interrupted executing TypeScript snippet", ie);
      Thread.currentThread().interrupt();
      return "(interrupted)";
    } catch (Exception e) {
      logger.error("Error executing TypeScript snippet", e);
      return "(error: " + e.getMessage() + ")";
    }
  }
}
