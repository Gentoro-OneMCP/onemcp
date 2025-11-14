package com.gentoro.onemcp.compiler;

import com.gentoro.onemcp.compiler.utils.InMemoryJavaFileObject;
import com.gentoro.onemcp.compiler.utils.MemoryClassLoader;
import com.gentoro.onemcp.compiler.utils.MemoryFileManager;
import com.gentoro.onemcp.exception.ExceptionUtil;
import com.gentoro.onemcp.orchestrator.OrchestratorContext;
import com.gentoro.onemcp.utility.StringUtility;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.tools.*;

public class JavaSnippetCompiler {

  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(JavaSnippetCompiler.class);

  private final JavaCompiler compiler;
  private final MemoryFileManager memoryFileManager;
  private final MemoryClassLoader memoryClassLoader;

  public JavaSnippetCompiler() {
    this.compiler = ToolProvider.getSystemJavaCompiler();
    if (this.compiler == null)
      throw new com.gentoro.onemcp.exception.StateException("No Java compiler available.");

    StandardJavaFileManager stdFileManager =
        compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8);
    this.memoryFileManager = new MemoryFileManager(stdFileManager);
    this.memoryClassLoader = new MemoryClassLoader(this.memoryFileManager);
  }

  public CompilationResult compileSnippet(String className, String fullCode) {
    try {
      log.trace(
          "Will attempt to compile snippet:\n\tClassname: {}.\n{}",
          className,
          StringUtility.formatWithIndent(fullCode, 4));
      JavaFileObject source = new InMemoryJavaFileObject(className, fullCode);
      DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

      long start = System.currentTimeMillis();
      JavaCompiler.CompilationTask task =
          compiler.getTask(
              null, memoryFileManager, diagnostics, null, null, Collections.singletonList(source));

      boolean success = task.call();
      log.trace(
          "Compilation task completed {} in ({}ms).",
          success ? "successfully" : "unsuccessfully",
          (System.currentTimeMillis() - start));

      if (!success) {
        StringBuilder errors = new StringBuilder("Compilation failed:\n");
        for (Diagnostic<?> d : diagnostics.getDiagnostics()) {
          errors.append(d.toString()).append("\n");
        }
        return new CompilationResult(false, className, fullCode, errors.toString());
      } else {
        return new CompilationResult(true, className, fullCode, null);
      }
    } catch (Exception e) {
      throw new com.gentoro.onemcp.exception.CompilationException("Failed to compile snippet", e);
    }
  }

  public ExecutionResult runSnippet(String className, OrchestratorContext context) {
    try {
      long start = System.currentTimeMillis();
      log.trace("Will execute snippet: {}", className);
      Class<SnippetBase> cls = (Class<SnippetBase>) memoryClassLoader.loadClass(className);
      SnippetBase instance = cls.getConstructor().newInstance();
      instance.setContext(context);
      Set<String> variables = instance.run();

      log.trace(
          "Snippet executed successfully. "
              + "Took ({}ms) to execute, and produced the following variables: {}",
          (System.currentTimeMillis() - start),
          String.join(", ", Objects.requireNonNullElse(variables, Collections.emptySet())));

      if (variables == null || variables.isEmpty()) {
        return new ExecutionResult(
            false,
            Collections.emptySet(),
            "Snippet executed successfully, but did not report any changed / added value to the value store. "
                + "This could indicate a problem with the implementation, review the implementation and make the necessary adjustments.");
      }
      return new ExecutionResult(true, variables, null);
    } catch (Exception e) {
      return new ExecutionResult(
          false, Collections.emptySet(), ExceptionUtil.formatCompactStackTrace(e));
    }
  }
}
