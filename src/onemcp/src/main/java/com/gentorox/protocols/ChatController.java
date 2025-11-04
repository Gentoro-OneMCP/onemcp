package com.gentorox.protocols;

import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.agent.Orchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Simple REST API for CLI chat mode.
 * Provides a straightforward HTTP endpoint to interact with the Orchestrator
 * without the complexity of the MCP protocol.
 */
@RestController
@RequestMapping("/api/chat")
public class ChatController {
  private static final Logger LOG = LoggerFactory.getLogger(ChatController.class);
  private final Orchestrator orchestrator;

  public ChatController(Orchestrator orchestrator) {
    this.orchestrator = orchestrator;
  }

  /**
   * Chat endpoint for CLI.
   * 
   * Request body:
   * {
   *   "message": "What are the revenues?",
   *   "options": { ... } // optional
   * }
   * 
   * Response:
   * {
   *   "content": "...",
   *   "traceId": "...",
   *   "error": null
   * }
   */
  @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public ChatResponse chat(@RequestBody ChatRequest request) {
    try {
      LOG.debug("Received chat request: {}", request.message());
      
      InferenceRequest.Message userMessage = new InferenceRequest.Message("user", request.message());
      Map<String, Object> options = request.options() != null ? request.options() : Map.of();
      
      InferenceResponse response = orchestrator.run(List.of(userMessage), options);
      
      return new ChatResponse(response.content(), response.providerTraceId(), null);
    } catch (Exception e) {
      LOG.error("Error processing chat request", e);
      return new ChatResponse(null, null, e.getMessage());
    }
  }

  /**
   * Health check endpoint for the chat API
   */
  @GetMapping("/health")
  public Map<String, String> health() {
    return Map.of("status", "ok");
  }

  /**
   * Request model
   */
  public record ChatRequest(
      String message,
      Map<String, Object> options
  ) {}

  /**
   * Response model
   */
  public record ChatResponse(
      String content,
      String traceId,
      String error
  ) {}
}

