package com.gentoro.onemcp.cache;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.StartupParameters;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Standalone command for creating dictionary from OpenAPI specifications.
 *
 * <p>Usage: java -jar onemcp.jar create-dict [handbook-path]
 */
public class CreateDictionaryCommand {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(CreateDictionaryCommand.class);

  public static void main(String[] args) {
    try {
      if (args.length < 1) {
        System.err.println("Usage: create-dict <handbook-path>");
        System.exit(1);
      }

      String handbookPathStr = args[0];
      Path handbookPath = Paths.get(handbookPathStr).toAbsolutePath();

      if (!java.nio.file.Files.exists(handbookPath)) {
        System.err.println("Handbook directory does not exist: " + handbookPath);
        System.exit(1);
      }

      // Create openapi/ directory if apis/ exists but openapi/ doesn't (for KnowledgeBase compatibility)
      // KnowledgeBase expects openapi/ directory, but CLI uses apis/
      Path apisDir = handbookPath.resolve("apis");
      Path openapiDir = handbookPath.resolve("openapi");
      if (java.nio.file.Files.exists(apisDir) && !java.nio.file.Files.exists(openapiDir)) {
        try {
          // Copy apis/ to openapi/ so KnowledgeBase can find the files
          // (Files.walk may not follow symlinks reliably)
          com.gentoro.onemcp.utility.FileUtility.copyDirectory(apisDir, openapiDir);
          log.debug("Copied apis/ to openapi/ for KnowledgeBase compatibility");
        } catch (Exception e) {
          log.warn("Could not copy apis to openapi: {}", e.getMessage());
          // Continue anyway - will fail later if KnowledgeBase can't find files
        }
      }

      // Create instructions.md if Agent.md exists but instructions.md doesn't (for KnowledgeBase compatibility)
      // KnowledgeBase expects instructions.md, but CLI uses Agent.md
      Path agentMd = handbookPath.resolve("Agent.md");
      Path instructionsMd = handbookPath.resolve("instructions.md");
      if (java.nio.file.Files.exists(agentMd) && !java.nio.file.Files.exists(instructionsMd)) {
        try {
          java.nio.file.Files.copy(agentMd, instructionsMd);
          log.debug("Copied Agent.md to instructions.md for KnowledgeBase compatibility");
        } catch (Exception e) {
          log.warn("Could not copy Agent.md to instructions.md: {}", e.getMessage());
          // Continue anyway - will fail validation if KnowledgeBase can't find instructions.md
        }
      }

      // Initialize OneMcp with minimal configuration
      // The configuration reads handbook.location from HANDBOOK_DIR environment variable
      // (see application.yaml: handbook.location: ${env:HANDBOOK_DIR:-classpath:acme-handbook})
      // We need to ensure HANDBOOK_DIR is set before initialization
      String originalHandbookDir = System.getenv("HANDBOOK_DIR");
      try {
        // Set HANDBOOK_DIR environment variable (will be read by configuration)
        // Note: We can't modify environment variables at runtime in Java, but the CLI
        // should set this before invoking the command. For standalone use, we'll rely
        // on the command-line argument being passed correctly.
        // The configuration will use the system property or we can override via config
        
        // For now, we'll pass the handbook path directly to the extractor
        // without full OneMcp initialization to avoid server startup overhead
        // Actually, we need OneMcp for LLM client and prompt repository, so let's initialize it
        
        // Set system property as fallback (though env var is preferred)
        System.setProperty("handbook.location", handbookPath.toString());
        
        String[] appArgs = new String[] {
            "--config", "classpath:application.yaml",
            "--mode", "server" // Use server mode to avoid interactive prompts
        };
        
        OneMcp oneMcp = new OneMcp(appArgs);
        oneMcp.initialize();

      // Extract dictionary
      DictionaryExtractorService extractor = new DictionaryExtractorService(oneMcp);
      PromptDictionary dictionary = extractor.extractDictionary(handbookPath);

      // Save to apis/dictionary.yaml
      Path outputPath = handbookPath.resolve("apis").resolve("dictionary.yaml");
      extractor.saveDictionary(dictionary, outputPath);

      System.out.println("Dictionary extracted and saved to: " + outputPath);
      System.out.println("Actions: " + dictionary.getActions().size());
      System.out.println("Entities: " + dictionary.getEntities().size());
      System.out.println("Fields: " + dictionary.getFields().size());

        oneMcp.shutdown();
        System.exit(0);
      } finally {
        // Restore original environment if it was set
        if (originalHandbookDir != null) {
          System.setProperty("handbook.location", originalHandbookDir);
        } else {
          System.clearProperty("handbook.location");
        }
      }
    } catch (Exception e) {
      System.err.println("Error creating dictionary: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}

