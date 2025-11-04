package com.gentorox.core.model;

import java.util.Optional;

public record InferenceResponse(String content, Optional<ToolCall> toolCall, String providerTraceId) {
  public record ToolCall(String toolName, String jsonArguments) {}
}
