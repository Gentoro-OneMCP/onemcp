package com.gentoro.onemcp.cache;

import static org.junit.jupiter.api.Assertions.*;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExecutionException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test for cache key stability of the PromptSchemaNormalizer.
 *
 * <p>This test defines clusters of prompts that should map to the same cache key (same action,
 * entities, and fields). It runs all prompts through the normalizer and verifies that prompts
 * within the same cluster produce identical cache keys.
 *
 * <p>This test uses REAL components - no mocking:
 * <ul>
 *   <li>Real OneMcp instance with actual LLM client
 *   <li>Real dictionary generation from Acme API handbook
 *   <li>Real normalizer calls with actual LLM inference
 * </ul>
 *
 * <p>Requires LLM configuration (API keys) to be set up.
 */
@DisplayName("PromptSchemaNormalizer Cache Stability Test (Real)")
class PromptSchemaNormalizerCacheStabilityTest {

  private OneMcp oneMcp;
  private PromptSchemaNormalizer normalizer;
  private PromptDictionary dictionary;
  private Path dictionaryPath;

  /**
   * Define clusters of prompts that should map to the same cache key. Each cluster contains
   * prompts that should normalize to the same schema (same action, entities, fields).
   */
  private static class PromptCluster {
    final String clusterName;
    final List<String> prompts;
    final String expectedAction;
    final List<String> expectedEntities;
    final List<String> expectedFields;

    PromptCluster(
        String clusterName,
        List<String> prompts,
        String expectedAction,
        List<String> expectedEntities,
        List<String> expectedFields) {
      this.clusterName = clusterName;
      this.prompts = prompts;
      this.expectedAction = expectedAction;
      this.expectedEntities = expectedEntities;
      this.expectedFields = expectedFields;
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    // Initialize real OneMcp instance
    String[] appArgs = new String[] {
        "--config", "classpath:application.yaml",
        "--mode", "server" // Use server mode to avoid interactive prompts
    };
    oneMcp = new OneMcp(appArgs);
    oneMcp.initialize();

    // Get or generate dictionary from Acme handbook
    dictionary = getOrGenerateDictionary();

    // Create real normalizer
    normalizer = new PromptSchemaNormalizer(oneMcp);
  }

  @AfterEach
  void tearDown() {
    if (oneMcp != null) {
      oneMcp.shutdown();
    }
  }

  /**
   * Get dictionary from cache or generate it from Acme handbook.
   * Dictionary is saved to target/test-reports/cache-stability/dictionary.yaml
   */
  private PromptDictionary getOrGenerateDictionary() throws IOException, ExecutionException {
    // Dictionary location in target directory
    Path reportsDir = Paths.get("target", "test-reports", "cache-stability");
    Files.createDirectories(reportsDir);
    dictionaryPath = reportsDir.resolve("dictionary.yaml");

    DictionaryExtractorService extractor = new DictionaryExtractorService(oneMcp);

    // Try to load existing dictionary
    PromptDictionary dict = extractor.loadDictionary(dictionaryPath);
    if (dict != null) {
      System.out.println("✅ Loaded existing dictionary from: " + dictionaryPath);
      System.out.println("   Actions: " + dict.getActions().size());
      System.out.println("   Entities: " + dict.getEntities().size());
      System.out.println("   Fields: " + dict.getFields().size());
      System.out.println("   Operators: " + (dict.getOperators() != null ? dict.getOperators().size() : 0));
      System.out.println("   Aggregates: " + (dict.getAggregates() != null ? dict.getAggregates().size() : 0));
      return dict;
    }

    // Dictionary doesn't exist, generate it from Acme handbook
    System.out.println("📝 Generating dictionary from Acme handbook...");
    Path acmeHandbookPath = Paths.get("src", "main", "resources", "acme-handbook");
    
    if (!Files.exists(acmeHandbookPath)) {
      throw new IOException("Acme handbook not found at: " + acmeHandbookPath.toAbsolutePath());
    }

    // Extract dictionary from handbook
    dict = extractor.extractDictionary(acmeHandbookPath);

    // Save dictionary for future test runs
    extractor.saveDictionary(dict, dictionaryPath);
    System.out.println("✅ Generated and saved dictionary to: " + dictionaryPath);
    System.out.println("   Actions: " + dict.getActions().size());
    System.out.println("   Entities: " + dict.getEntities().size());
    System.out.println("   Fields: " + dict.getFields().size());
    System.out.println("   Operators: " + (dict.getOperators() != null ? dict.getOperators().size() : 0));
    System.out.println("   Aggregates: " + (dict.getAggregates() != null ? dict.getAggregates().size() : 0));

    return dict;
  }

  /**
   * Create a dictionary based on the Acme Sales Analytics API specification.
   * NOTE: This method is no longer used - we now generate from the real handbook.
   */
  @SuppressWarnings("unused")
  private PromptDictionary createAcmeDictionary() {
    PromptDictionary dict = new PromptDictionary();

    // Actions from the API
    dict.setActions(
        Arrays.asList(
            "search", "get", "list", "summarize", "rank", "create", "update", "delete", "trigger"));

    // Entities from the API
    dict.setEntities(
        Arrays.asList(
            "sale", "product", "customer", "region", "date", "transaction", "order", "payment"));

    // Fields from the API (sample - full list would include all fields from the OpenAPI spec)
    dict.setFields(
        Arrays.asList(
            "sale.id",
            "sale.amount",
            "sale.date",
            "sale.quantity",
            "sale.discount",
            "sale.tax",
            "sale.shipping_cost",
            "sale.payment_method",
            "sale.status",
            "product.id",
            "product.name",
            "product.category",
            "product.subcategory",
            "product.brand",
            "product.price",
            "product.cost",
            "product.inventory",
            "product.rating",
            "customer.id",
            "customer.name",
            "customer.email",
            "customer.age",
            "customer.gender",
            "customer.city",
            "customer.state",
            "customer.country",
            "customer.zip_code",
            "customer.loyalty_tier",
            "customer.total_spent",
            "customer.total_orders",
            "region.name",
            "region.country",
            "date.year",
            "date.month",
            "date.quarter",
            "date.week",
            "date.day_of_week"));

    // Operators
    dict.setOperators(
        Arrays.asList(
            "equals",
            "not_equals",
            "greater_than",
            "greater_than_or_equal",
            "less_than",
            "less_than_or_equal",
            "contains",
            "not_contains",
            "starts_with",
            "ends_with",
            "in",
            "not_in",
            "between",
            "is_null",
            "is_not_null"));

    // Aggregates
    dict.setAggregates(
        Arrays.asList("sum", "avg", "count", "min", "max", "median", "stddev", "variance"));

    return dict;
  }

  /**
   * Define prompt clusters based on the Acme API. Each cluster contains prompts that should
   * normalize to the same schema structure.
   * 
   * NOTE: Group by clusters are placed first to catch issues early and save tokens.
   */
  private List<PromptCluster> createPromptClusters() {
    List<PromptCluster> clusters = new ArrayList<>();

    // Cluster 1: Summarize sales by customer state (GROUP BY - placed FIRST due to parsing issues)
    clusters.add(
        new PromptCluster(
            "Summarize sales by customer state",
            Arrays.asList(
                "Calculate total sales by state",
                "What's the total revenue per state?",
                "Sum up sales for each state",
                "Show me sales totals grouped by customer state",
                "Aggregate revenue by state"),
            "summarize",
            Arrays.asList("sale", "customer"),
            Arrays.asList("customer_state", "sale_amount")));

    // Cluster 2: Summarize revenue by category (GROUP BY - placed second)
    clusters.add(
        new PromptCluster(
            "Summarize revenue by category",
            Arrays.asList(
                "Calculate total revenue by product category",
                "What's the total sales revenue grouped by category?",
                "Sum up revenue for each product category",
                "Show me revenue totals by category",
                "Aggregate sales amount by product category"),
            "summarize",
            Arrays.asList("sale", "product"),
            Arrays.asList("product_category", "sale_amount")));

    // Cluster 3: Search sales by category and state
    clusters.add(
        new PromptCluster(
            "Search sales by category and state",
            Arrays.asList(
                "Show me electronics sales in California",
                "Find electronics sales in CA",
                "Get sales data for electronics in California",
                "List electronics transactions in California state",
                "Search for electronic product sales in California"),
            "search",
            Arrays.asList("sale", "product", "customer"),
            Arrays.asList("product_category", "customer_state")));

    // Cluster 4: Search sales by date range
    clusters.add(
        new PromptCluster(
            "Search sales by date range",
            Arrays.asList(
                "Show me sales from Q4 2023",
                "Find sales in the fourth quarter of 2023",
                "Get sales data for Q4 2023",
                "List transactions from Q4 2023",
                "Search for sales in quarter 4 of 2023"),
            "search",
            Arrays.asList("sale", "date"),
            Arrays.asList("date_quarter", "date_year")));

    // Cluster 5: Search high-value sales
    clusters.add(
        new PromptCluster(
            "Search high-value sales",
            Arrays.asList(
                "Find sales over $500",
                "Show me sales greater than 500 dollars",
                "Get sales where amount is more than $500",
                "List transactions above $500",
                "Search for sales exceeding 500 dollars"),
            "search",
            Arrays.asList("sale"),
            Arrays.asList("sale_amount")));

    // Cluster 6: Summarize customer loyalty analysis (GROUP BY - moved up)
    clusters.add(
        new PromptCluster(
            "Summarize customer loyalty analysis",
            Arrays.asList(
                "Calculate total spending by loyalty tier",
                "What's the total revenue per loyalty tier?",
                "Sum up sales for each customer loyalty tier",
                "Show me revenue totals grouped by loyalty tier",
                "Aggregate sales by customer loyalty level"),
            "summarize",
            Arrays.asList("sale", "customer"),
            Arrays.asList("customer_loyalty_tier", "sale_amount")));

    // Cluster 7: Search sales by payment method
    clusters.add(
        new PromptCluster(
            "Search sales by payment method",
            Arrays.asList(
                "Show me credit card sales",
                "Find sales paid with credit card",
                "Get transactions using credit card payment",
                "List sales where payment method is credit card",
                "Search for credit card transactions"),
            "search",
            Arrays.asList("sale"),
            Arrays.asList("sale_payment_method")));

    // Cluster 8: Summarize product performance (GROUP BY - moved up)
    clusters.add(
        new PromptCluster(
            "Summarize product performance",
            Arrays.asList(
                "Calculate total sales by product name",
                "What's the total revenue per product?",
                "Sum up sales for each product",
                "Show me revenue totals grouped by product name",
                "Aggregate sales by product"),
            "summarize",
            Arrays.asList("sale", "product"),
            Arrays.asList("product_name", "sale_amount")));

    // Reorder: Move all GROUP BY clusters (summarize with grouping) to the front
    // This helps catch issues early and save tokens
    List<PromptCluster> reordered = new ArrayList<>();
    List<PromptCluster> groupByClusters = new ArrayList<>();
    List<PromptCluster> otherClusters = new ArrayList<>();
    
    for (PromptCluster cluster : clusters) {
      if ("summarize".equals(cluster.expectedAction) && 
          cluster.expectedFields.size() > 1) { // Group by clusters have multiple fields
        groupByClusters.add(cluster);
      } else {
        otherClusters.add(cluster);
      }
    }
    
    // Put group by clusters first, then others
    reordered.addAll(groupByClusters);
    reordered.addAll(otherClusters);
    
    return reordered;
  }

  /**
   * Create a mock LLM response that returns a normalized schema matching the expected cluster.
   * NOTE: This method is no longer used - we now use real LLM calls.
   */
  @SuppressWarnings("unused")
  private String createMockLlmResponse(PromptCluster cluster) {
    // Build the expected JSON response
    StringBuilder json = new StringBuilder();
    json.append("{\n");
    json.append("  \"workflow_type\": \"sequential\",\n");
    json.append("  \"steps\": [\n");
    json.append("    {\n");
    json.append("      \"ps\": {\n");
    json.append("        \"action\": \"").append(cluster.expectedAction).append("\",\n");
    json.append("        \"entities\": [");
    json.append(
        cluster.expectedEntities.stream()
            .map(e -> "\"" + e + "\"")
            .collect(Collectors.joining(", ")));
    json.append("],\n");
    json.append("        \"fields\": [");
    json.append(
        cluster.expectedFields.stream()
            .map(f -> "\"" + f + "\"")
            .collect(Collectors.joining(", ")));
    json.append("],\n");
    json.append("        \"params\": {\n");
    // Add sample params (these don't affect cache key)
    for (int i = 0; i < cluster.expectedFields.size(); i++) {
      String field = cluster.expectedFields.get(i);
      json.append("          \"").append(field).append("\": \"sample_value\"");
      if (i < cluster.expectedFields.size() - 1) {
        json.append(",");
      }
      json.append("\n");
    }
    json.append("        }\n");
    json.append("      }\n");
    json.append("    }\n");
    json.append("  ]\n");
    json.append("}\n");
    return json.toString();
  }

  /**
   * Data structure to hold cluster test results for report generation.
   */
  private static class ClusterResult {
    final PromptCluster cluster;
    final Map<String, String> promptToCacheKey;
    final Map<String, PromptSchema> promptToSchema;
    final Set<String> uniqueCacheKeys;
    final boolean success;

    ClusterResult(
        PromptCluster cluster,
        Map<String, String> promptToCacheKey,
        Map<String, PromptSchema> promptToSchema,
        Set<String> uniqueCacheKeys) {
      this.cluster = cluster;
      this.promptToCacheKey = promptToCacheKey;
      this.promptToSchema = promptToSchema;
      this.uniqueCacheKeys = uniqueCacheKeys;
      this.success = uniqueCacheKeys.size() == 1;
    }
  }

  @Test
  @DisplayName("Test cache key stability - detailed analysis with report")
  void testCacheKeyStabilityDetailed() throws Exception {
    List<PromptCluster> clusters = createPromptClusters();

    System.out.println("\n=== Cache Key Stability Analysis ===\n");

    int totalClusters = clusters.size();
    int successfulClusters = 0;
    int totalPrompts = 0;
    int successfulPrompts = 0;

    List<ClusterResult> results = new ArrayList<>();

    for (PromptCluster cluster : clusters) {
      System.out.println("Cluster: " + cluster.clusterName);
      System.out.println("  Expected: action=" + cluster.expectedAction);
      System.out.println("            entities=" + cluster.expectedEntities);
      System.out.println("            fields=" + cluster.expectedFields);

      Map<String, String> promptToCacheKey = new HashMap<>();
      Map<String, PromptSchema> promptToSchema = new HashMap<>();
      Set<String> uniqueCacheKeys = new HashSet<>();

      for (String prompt : cluster.prompts) {
        totalPrompts++;
        System.out.println("  Processing: \"" + prompt + "\"...");

        // Normalize the prompt with REAL LLM call
        PromptSchemaWorkflow workflow = normalizer.normalize(prompt, dictionary);

        String cacheKey = null;
        PromptSchema schema = null;
        if (workflow != null && workflow.getSteps() != null && !workflow.getSteps().isEmpty()) {
          PromptSchema ps = workflow.getSteps().get(0).getPs();
          if (ps != null && ps.getCacheKey() != null) {
            cacheKey = ps.getCacheKey();
            schema = ps;
            uniqueCacheKeys.add(cacheKey);
            successfulPrompts++;
          }
        }

        promptToCacheKey.put(prompt, cacheKey);
        if (schema != null) {
          promptToSchema.put(prompt, schema);
        }
      }

      boolean clusterSuccess = uniqueCacheKeys.size() == 1;
      if (clusterSuccess) {
        successfulClusters++;
      }

      results.add(
          new ClusterResult(cluster, promptToCacheKey, promptToSchema, uniqueCacheKeys));

      System.out.println("  Prompts tested: " + cluster.prompts.size());
      System.out.println("  Unique cache keys: " + uniqueCacheKeys.size());
      System.out.println(
          "  Status: " + (clusterSuccess ? "✅ PASS" : "❌ FAIL - keys differ"));
      
      // Show cache keys
      if (clusterSuccess && !uniqueCacheKeys.isEmpty()) {
        System.out.println("  Cache Key: " + uniqueCacheKeys.iterator().next());
      } else if (!uniqueCacheKeys.isEmpty()) {
        System.out.println("  Cache Keys found:");
        for (String key : uniqueCacheKeys) {
          System.out.println("    - " + key);
        }
        // Show which prompt produced which key
        Map<String, List<String>> keyToPrompts = new HashMap<>();
        for (Map.Entry<String, String> entry : promptToCacheKey.entrySet()) {
          String key = entry.getValue() != null ? entry.getValue() : "null";
          keyToPrompts.computeIfAbsent(key, k -> new ArrayList<>()).add(entry.getKey());
        }
        for (Map.Entry<String, List<String>> entry : keyToPrompts.entrySet()) {
          System.out.println("    Key: " + entry.getKey());
          for (String prompt : entry.getValue()) {
            System.out.println("      → \"" + prompt + "\"");
          }
        }
      }
      System.out.println();
    }

    System.out.println("=== Summary ===");
    System.out.println("Total clusters: " + totalClusters);
    System.out.println("Successful clusters: " + successfulClusters);
    System.out.println("Total prompts: " + totalPrompts);
    System.out.println("Successful prompts: " + successfulPrompts);
    System.out.println(
        "Success rate: "
            + (totalClusters > 0
                ? String.format("%.1f%%", (successfulClusters * 100.0 / totalClusters))
                : "N/A"));

    // Generate report
    String reportPath = generateReport(results, totalClusters, successfulClusters, totalPrompts, successfulPrompts);
    
    // Print prominent report location
    System.out.println("\n" + "=".repeat(80));
    System.out.println("📄 CACHE STABILITY TEST REPORT");
    System.out.println("=".repeat(80));
    System.out.println("\nReport Location:");
    System.out.println("  " + reportPath);
    System.out.println("\nTo open the report:");
    System.out.println("  macOS:   open " + reportPath);
    System.out.println("  Linux:   xdg-open " + reportPath);
    System.out.println("  Windows: start " + reportPath.replace("/", "\\"));
    System.out.println("\nOr copy the path above and open it in your markdown viewer.");
    System.out.println("=".repeat(80) + "\n");

    // Assert overall success
    assertEquals(
        totalClusters,
        successfulClusters,
        "All clusters should produce stable cache keys. "
            + (totalClusters - successfulClusters)
            + " cluster(s) failed.");
  }

  /**
   * Generate a comprehensive markdown report of the cache stability test results.
   */
  private String generateReport(
      List<ClusterResult> results,
      int totalClusters,
      int successfulClusters,
      int totalPrompts,
      int successfulPrompts)
      throws IOException {
    // Create reports directory in target
    Path reportsDir = Paths.get("target", "test-reports", "cache-stability");
    Files.createDirectories(reportsDir);

    // Generate filename with timestamp
    String timestamp =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
    Path reportFile = reportsDir.resolve("cache-stability-report_" + timestamp + ".md");

    StringBuilder report = new StringBuilder();

    // Header
    report.append("# Cache Key Stability Test Report\n\n");
    report.append("**Generated:** ")
        .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
        .append("\n\n");
    report.append("---\n\n");

    // Executive Summary
    report.append("## Executive Summary\n\n");
    double successRate =
        totalClusters > 0 ? (successfulClusters * 100.0 / totalClusters) : 0.0;
    String statusEmoji = successRate == 100.0 ? "✅" : successRate >= 80.0 ? "⚠️" : "❌";
    report.append(statusEmoji)
        .append(" **Overall Status: ")
        .append(successRate == 100.0 ? "PASS" : "NEEDS ATTENTION")
        .append("**\n\n");

    report.append("| Metric | Value |\n");
    report.append("|--------|-------|\n");
    report.append("| Total Clusters | ").append(totalClusters).append(" |\n");
    report.append("| Successful Clusters | ").append(successfulClusters).append(" |\n");
    report.append("| Failed Clusters | ")
        .append(totalClusters - successfulClusters)
        .append(" |\n");
    report.append("| Success Rate | ")
        .append(String.format("%.1f%%", successRate))
        .append(" |\n");
    report.append("| Total Prompts Tested | ").append(totalPrompts).append(" |\n");
    report.append("| Successful Prompts | ").append(successfulPrompts).append(" |\n");
    report.append("\n");

    // Detailed Results by Cluster
    report.append("## Detailed Results by Cluster\n\n");

    for (int i = 0; i < results.size(); i++) {
      ClusterResult result = results.get(i);
      PromptCluster cluster = result.cluster;

      report.append("### ").append(i + 1).append(". ").append(cluster.clusterName).append("\n\n");

      String status = result.success ? "✅ **PASS**" : "❌ **FAIL**";
      report.append("**Status:** ").append(status).append("\n\n");

      report.append("**Expected Schema:**\n");
      report.append("- **Action:** `").append(cluster.expectedAction).append("`\n");
      report.append("- **Entities:** `")
          .append(String.join(", ", cluster.expectedEntities))
          .append("`\n");
      report.append("- **Fields:** `")
          .append(String.join(", ", cluster.expectedFields))
          .append("`\n\n");

      report.append("**Test Results:**\n");
      report.append("- Prompts tested: ").append(cluster.prompts.size()).append("\n");
      report.append("- Unique cache keys found: ")
          .append(result.uniqueCacheKeys.size())
          .append("\n\n");

      if (result.success) {
        String cacheKey = result.uniqueCacheKeys.iterator().next();
        report.append("**✅ All prompts produced the same cache key:**\n");
        report.append("```\n").append(cacheKey).append("\n```\n\n");
      } else {
        report.append("**❌ Cache key mismatches detected:**\n\n");
        // Group prompts by cache key
        Map<String, List<String>> cacheKeyToPrompts = new HashMap<>();
        for (Map.Entry<String, String> entry : result.promptToCacheKey.entrySet()) {
          String key = entry.getValue() != null ? entry.getValue() : "null";
          cacheKeyToPrompts.computeIfAbsent(key, k -> new ArrayList<>()).add(entry.getKey());
        }

        for (Map.Entry<String, List<String>> entry : cacheKeyToPrompts.entrySet()) {
          report.append("**Cache Key:** `").append(entry.getKey()).append("`\n");
          report.append("**Prompts:**\n");
          for (String prompt : entry.getValue()) {
            report.append("- \"").append(prompt).append("\"\n");
          }
          report.append("\n");
        }
      }

      // Show actual schemas for each prompt
      report.append("**Actual Schemas Generated:**\n\n");
      for (String prompt : cluster.prompts) {
        PromptSchema schema = result.promptToSchema.get(prompt);
        String cacheKey = result.promptToCacheKey.get(prompt);

        report.append("**Prompt:** \"").append(prompt).append("\"\n");
        if (schema != null) {
          report.append("- Action: `").append(schema.getAction()).append("`\n");
          report.append("- Entities: `")
              .append(String.join(", ", schema.getEntities()))
              .append("`\n");
          if (schema.getParams() != null && !schema.getParams().isEmpty()) {
            report.append("- Params: `")
                .append(String.join(", ", schema.getParams().keySet()))
                .append("`\n");
          }
          if (schema.getGroupBy() != null && !schema.getGroupBy().isEmpty()) {
            report.append("- Group By: `")
                .append(String.join(", ", schema.getGroupBy()))
                .append("`\n");
          }
          report.append("- Cache Key: `").append(cacheKey != null ? cacheKey : "null").append("`\n");
        } else {
          report.append("- ⚠️ No schema generated\n");
        }
        report.append("\n");
      }

      report.append("---\n\n");
    }

    // Recommendations
    report.append("## Recommendations\n\n");

    if (successRate == 100.0) {
      report.append("✅ **Excellent!** All clusters show perfect cache key stability.\n");
      report.append("The normalizer is working correctly and producing consistent cache keys.\n\n");
    } else if (successRate >= 80.0) {
      report.append("⚠️ **Good, but needs improvement.** Most clusters are stable.\n");
      report.append("Review the failed clusters above and consider:\n");
      report.append("- Refining the normalization prompt to be more consistent\n");
      report.append("- Adjusting the dictionary to better capture semantic equivalence\n");
      report.append("- Reviewing field/entity extraction logic\n\n");
    } else {
      report.append("❌ **Needs significant attention.** Many clusters show instability.\n");
      report.append("Consider:\n");
      report.append("- Major review of the normalization prompt\n");
      report.append("- Dictionary refinement\n");
      report.append("- Field/entity extraction improvements\n");
      report.append("- Cache key generation algorithm review\n\n");
    }

    // Footer
    report.append("---\n\n");
    report.append("*Report generated by PromptSchemaNormalizerCacheStabilityTest*\n");

    // Write report
    Files.writeString(reportFile, report.toString());

    return reportFile.toAbsolutePath().toString();
  }
}

