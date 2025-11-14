package com.gentoro.onemcp.compiler;

public record CompilationResult(
    boolean success, String className, String fullSnippet, String reportedErrors) {}
