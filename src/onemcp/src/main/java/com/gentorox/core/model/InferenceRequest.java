package com.gentorox.core.model;

import java.util.List;
import java.util.Map;

public record InferenceRequest(String model, List<Message> messages, Map<String, Object> options) {
  public record Message(String role, Object content) {}
}
