package com.gentoro.onemcp.compiler;

import java.util.Set;

public record ExecutionResult(boolean success, Set<String> variables, String reportedErrors) {}
