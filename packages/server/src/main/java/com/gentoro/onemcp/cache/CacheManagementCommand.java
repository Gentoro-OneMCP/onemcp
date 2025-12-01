package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gentoro.onemcp.utility.JacksonUtility;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * CLI command for managing execution plan cache.
 * 
 * Usage:
 *   java -cp ... com.gentoro.onemcp.cache.CacheManagementCommand list [handbook_path]
 *   java -cp ... com.gentoro.onemcp.cache.CacheManagementCommand remove <cache_key|index> [handbook_path]
 *   java -cp ... com.gentoro.onemcp.cache.CacheManagementCommand clear [handbook_path]
 */
public class CacheManagementCommand {
  private static final ObjectMapper mapper = JacksonUtility.getJsonMapper();
  public static void main(String[] args) {
    if (args.length == 0) {
      printUsage();
      System.exit(1);
    }

    String command = args[0];
    Path handbookPath = null;
    
    // Parse handbook path (optional, for determining cache directory)
    if (args.length > 1 && !args[args.length - 1].startsWith("-")) {
      handbookPath = Paths.get(args[args.length - 1]);
    } else {
      // Try to get from environment or use default
      String homeDir = System.getenv("ONEMCP_HOME_DIR");
      if (homeDir != null && !homeDir.isBlank()) {
        handbookPath = Paths.get(homeDir);
      } else {
        String userHome = System.getProperty("user.home");
        if (userHome != null) {
          handbookPath = Paths.get(userHome, ".onemcp");
        }
      }
    }

    ExecutionPlanCache cache = new ExecutionPlanCache(handbookPath != null ? handbookPath : Paths.get("."));

    try {
      switch (command) {
        case "list":
          listCachedPlans(cache);
          break;
        case "remove":
          if (args.length < 2) {
            System.err.println("Error: cache key or index required for remove command");
            printUsage();
            System.exit(1);
          }
          String cacheKeyOrIndex = args[1];
          removeCachedPlan(cache, cacheKeyOrIndex);
          break;
        case "clear":
          clearCache(cache);
          break;
        default:
          System.err.println("Error: Unknown command: " + command);
          printUsage();
          System.exit(1);
      }
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void listCachedPlans(ExecutionPlanCache cache) throws Exception {
    Path cacheDir = cache.getCacheDirectory();
    System.out.println("Cache directory: " + cacheDir);
    System.out.println();

    if (!Files.exists(cacheDir)) {
      System.out.println("No cache directory found.");
      return;
    }

    try (Stream<Path> files = Files.list(cacheDir)) {
      var planFiles = files
          .filter(p -> p.toString().endsWith(".json"))
          .sorted()
          .toList();

      if (planFiles.isEmpty()) {
        System.out.println("No cached plans found.");
        return;
      }

      System.out.println("Cached plans (" + planFiles.size() + "):");
      System.out.println();
      
      List<CacheEntry> entries = new ArrayList<>();
      for (int i = 0; i < planFiles.size(); i++) {
        Path file = planFiles.get(i);
        String cacheKey = file.getFileName().toString().replace(".json", "");
        CacheEntry entry = parseCacheEntry(file, cacheKey, i + 1);
        entries.add(entry);
      }

      // Display entries with details
      for (CacheEntry entry : entries) {
        System.out.println(String.format("[%d] %s", entry.index, entry.cacheKey));
        if (entry.operation != null && !entry.operation.isEmpty()) {
          System.out.println("    Operation: " + entry.operation);
        }
        if (entry.summary != null && !entry.summary.isEmpty()) {
          System.out.println("    Summary: " + entry.summary);
        }
        if (entry.status != null) {
          System.out.println("    Status: " + entry.status);
          if (entry.failedAttempts > 0) {
            System.out.println("    Failed attempts: " + entry.failedAttempts);
          }
        }
        if (entry.createdAt != null) {
          System.out.println("    Created: " + entry.createdAt);
        }
        System.out.println();
      }
      
      System.out.println("To remove a plan, use:");
      System.out.println("  onemcp cache remove <index>  (e.g., 'onemcp cache remove 1')");
      System.out.println("  onemcp cache remove <cache_key>  (e.g., 'onemcp cache remove query-sale-...')");
    }
  }

  private static CacheEntry parseCacheEntry(Path file, String cacheKey, int index) {
    CacheEntry entry = new CacheEntry();
    entry.index = index;
    entry.cacheKey = cacheKey;
    
    try {
      String content = Files.readString(file);
      JsonNode root = mapper.readTree(content);
      
      // Extract metadata
      if (root.has("created_at")) {
        entry.createdAt = root.get("created_at").asText();
      }
      if (root.has("execution_status")) {
        entry.status = root.get("execution_status").asText();
      }
      if (root.has("failed_attempts") && !root.get("failed_attempts").isNull()) {
        entry.failedAttempts = root.get("failed_attempts").asInt();
      }
      
      // Parse plan JSON to extract identifying information
      if (root.has("plan")) {
        String planJson = root.get("plan").asText();
        JsonNode planNode = mapper.readTree(planJson);
        
        // Extract operation names from nodes
        List<String> operations = new ArrayList<>();
        if (planNode.isObject()) {
          extractOperations(planNode, operations);
        }
        if (!operations.isEmpty()) {
          entry.operation = String.join(", ", operations);
        }
        
        // Extract summary/terminal node info
        if (planNode.has("terminal")) {
          JsonNode terminal = planNode.get("terminal");
          if (terminal.has("vars")) {
            JsonNode vars = terminal.get("vars");
            if (vars.has("answer")) {
              String answerPath = vars.get("answer").asText();
              entry.summary = "answer: " + answerPath;
            }
          }
        }
      }
    } catch (Exception e) {
      // If parsing fails, just show the cache key
      entry.operation = "(unable to parse plan)";
    }
    
    return entry;
  }

  private static void extractOperations(JsonNode node, List<String> operations) {
    if (node.isObject()) {
      // Check if this is an operation node
      if (node.has("operation")) {
        String op = node.get("operation").asText();
        if (!operations.contains(op)) {
          operations.add(op);
        }
      }
      
      // Recursively check all fields
      node.fields().forEachRemaining(entry -> {
        extractOperations(entry.getValue(), operations);
      });
    } else if (node.isArray()) {
      for (JsonNode item : node) {
        extractOperations(item, operations);
      }
    }
  }

  private static class CacheEntry {
    int index;
    String cacheKey;
    String operation;
    String summary;
    String status;
    String createdAt;
    int failedAttempts;
  }

  private static void removeCachedPlan(ExecutionPlanCache cache, String cacheKeyOrIndex) throws Exception {
    Path cacheDir = cache.getCacheDirectory();
    String actualCacheKey = cacheKeyOrIndex;
    
    // Check if it's a numeric index
    try {
      int index = Integer.parseInt(cacheKeyOrIndex);
      // List files to find the one at this index
      try (Stream<Path> files = Files.list(cacheDir)) {
        var planFiles = files
            .filter(p -> p.toString().endsWith(".json"))
            .sorted()
            .toList();
        
        if (index < 1 || index > planFiles.size()) {
          System.err.println("Error: Index " + index + " is out of range (1-" + planFiles.size() + ")");
          System.err.println("Run 'onemcp cache list' to see available plans.");
          System.exit(1);
        }
        
        Path file = planFiles.get(index - 1);
        actualCacheKey = file.getFileName().toString().replace(".json", "");
      }
    } catch (NumberFormatException e) {
      // Not a number, treat as cache key
      actualCacheKey = cacheKeyOrIndex;
    }
    
    boolean removed = cache.removeByCacheKey(actualCacheKey);
    if (removed) {
      System.out.println("Removed cached plan: " + actualCacheKey);
    } else {
      System.err.println("Failed to remove cached plan: " + actualCacheKey + " (not found or error occurred)");
      System.exit(1);
    }
  }

  private static void clearCache(ExecutionPlanCache cache) {
    int count = cache.clear();
    System.out.println("Cleared " + count + " cached plan(s)");
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  java com.gentoro.onemcp.cache.CacheManagementCommand <command> [args] [handbook_path]");
    System.out.println();
    System.out.println("Commands:");
    System.out.println("  list                    List all cached plans with details");
    System.out.println("  remove <key|index>      Remove a cached plan by cache key or index (from list)");
    System.out.println("  clear                   Clear all cached plans");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  java ... CacheManagementCommand list");
    System.out.println("  java ... CacheManagementCommand remove 1");
    System.out.println("  java ... CacheManagementCommand remove query-sale-filter:date_year:equals-param:date_year");
    System.out.println("  java ... CacheManagementCommand clear");
  }
}

