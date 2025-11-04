package com.gentorox.services.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Service for validating foundation folder structure and content.
 * Validates Agent.md format, documentation presence, and OpenAPI spec validity.
 */
@Service
public class FoundationValidationService {
    private static final Logger log = LoggerFactory.getLogger(FoundationValidationService.class);
    
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final ObjectMapper jsonMapper = new ObjectMapper();
    
    // Patterns for Agent.md validation
    private static final Pattern AGENT_TITLE_PATTERN = Pattern.compile("^#\\s+.*Agent.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern AGENT_PURPOSE_PATTERN = Pattern.compile(".*purpose.*|.*goal.*|.*objective.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern AGENT_BEHAVIOR_PATTERN = Pattern.compile(".*behavior.*|.*guidance.*|.*rules.*", Pattern.CASE_INSENSITIVE);
    
    /**
     * Validates the foundation directory structure and content.
     * 
     * @param foundationDir The foundation directory path
     * @return ValidationResult containing validation status and details
     */
    public ValidationResult validateFoundation(String foundationDir) {
        log.info("Starting foundation validation for directory: {}", foundationDir);
        
        ValidationResult result = new ValidationResult();
        File root = new File(foundationDir);
        
        // Check if foundation directory exists
        if (!root.exists()) {
            result.addError("Foundation directory not found: " + root.getAbsolutePath());
            return result;
        }
        
        if (!root.isDirectory()) {
            result.addError("Foundation path is not a directory: " + root.getAbsolutePath());
            return result;
        }
        
        // Validate Agent.md
        validateAgentMd(root, result);
        
        // Validate documentation files
        validateDocumentation(root, result);
        
        // Validate OpenAPI specifications
        validateOpenApiSpecs(root, result);
        
        log.info("Foundation validation completed. Errors: {}, Warnings: {}", 
                result.getErrors().size(), result.getWarnings().size());
        
        return result;
    }
    
    /**
     * Validates Agent.md file presence and format.
     */
    private void validateAgentMd(File root, ValidationResult result) {
        File agentFile = new File(root, "Agent.md");
        
        if (!agentFile.exists()) {
            result.addError("Agent.md is missing from foundation directory");
            return;
        }
        
        if (agentFile.length() == 0) {
            result.addError("Agent.md is empty");
            return;
        }
        
        try {
            String content = Files.readString(agentFile.toPath());
            validateAgentMdFormat(content, result);
            result.addSuccess("✓ Agent.md found and validated");
        } catch (IOException e) {
            result.addError("Failed to read Agent.md: " + e.getMessage());
        }
    }
    
    /**
     * Validates Agent.md content format and structure.
     */
    private void validateAgentMdFormat(String content, ValidationResult result) {
        String[] lines = content.split("\n");
        boolean hasTitle = false;
        boolean hasPurpose = false;
        boolean hasBehavior = false;
        boolean hasTools = false;
        
        for (String line : lines) {
            String trimmed = line.trim();
            
            // Check for title (should start with # and contain "Agent")
            if (AGENT_TITLE_PATTERN.matcher(trimmed).matches()) {
                hasTitle = true;
            }
            
            // Check for purpose/goal/objective section
            if (AGENT_PURPOSE_PATTERN.matcher(trimmed).matches()) {
                hasPurpose = true;
            }
            
            // Check for behavior/guidance section
            if (AGENT_BEHAVIOR_PATTERN.matcher(trimmed).matches()) {
                hasBehavior = true;
            }
            
            // Check for tools section
            if (trimmed.toLowerCase().contains("tools") || trimmed.toLowerCase().contains("tool")) {
                hasTools = true;
            }
        }
        
        // Validate required sections
        if (!hasTitle) {
            result.addWarning("Agent.md should have a title starting with '# Agent' or similar");
        }
        
        if (!hasPurpose) {
            result.addWarning("Agent.md should describe the agent's purpose, goal, or objective");
        }
        
        if (!hasBehavior) {
            result.addWarning("Agent.md should include behavioral guidance or rules");
        }
        
        if (!hasTools) {
            result.addWarning("Agent.md should mention available tools or capabilities");
        }
        
        // Check minimum content length
        if (content.length() < 100) {
            result.addWarning("Agent.md seems too short - consider adding more detailed instructions");
        }
    }
    
    /**
     * Validates presence of documentation files.
     */
    private void validateDocumentation(File root, ValidationResult result) {
        File docsDir = new File(root, "docs");
        
        if (!docsDir.exists() || !docsDir.isDirectory()) {
            result.addError("docs/ directory is missing - at least one documentation file is required");
            return;
        }
        
        File[] mdFiles = docsDir.listFiles((dir, name) -> 
            name.toLowerCase().endsWith(".md") || name.toLowerCase().endsWith(".mdx"));
        
        if (mdFiles == null || mdFiles.length == 0) {
            result.addError("No markdown documentation files found in docs/ directory");
            return;
        }
        
        result.addSuccess("✓ Documentation found: " + mdFiles.length + " file(s)");
        
        // Check for common documentation patterns
        boolean hasOverview = false;
        boolean hasApiDocs = false;
        
        for (File file : mdFiles) {
            String name = file.getName().toLowerCase();
            if (name.contains("overview") || name.contains("readme") || name.contains("introduction")) {
                hasOverview = true;
            }
            if (name.contains("api") || name.contains("endpoint") || name.contains("reference")) {
                hasApiDocs = true;
            }
        }
        
        if (!hasOverview) {
            result.addWarning("Consider adding an overview or README file in docs/");
        }
        
        if (!hasApiDocs) {
            result.addWarning("Consider adding API documentation in docs/");
        }
    }
    
    /**
     * Validates OpenAPI specifications.
     */
    private void validateOpenApiSpecs(File root, ValidationResult result) {
        File apisDir = new File(root, "apis");
        
        if (!apisDir.exists() || !apisDir.isDirectory()) {
            result.addError("apis/ directory is missing - OpenAPI specification is required");
            return;
        }
        
        File[] specFiles = apisDir.listFiles((dir, name) -> 
            name.toLowerCase().endsWith(".yaml") || 
            name.toLowerCase().endsWith(".yml") || 
            name.toLowerCase().endsWith(".json"));
        
        if (specFiles == null || specFiles.length == 0) {
            result.addError("No OpenAPI specification files found in apis/ directory");
            return;
        }
        
        boolean hasValidSpec = false;
        for (File specFile : specFiles) {
            if (validateOpenApiFile(specFile, result)) {
                hasValidSpec = true;
            }
        }
        
        if (!hasValidSpec) {
            result.addError("No valid OpenAPI specification found in apis/ directory");
        } else {
            result.addSuccess("✓ Valid OpenAPI specification found");
        }
    }
    
    /**
     * Validates a single OpenAPI specification file.
     */
    private boolean validateOpenApiFile(File specFile, ValidationResult result) {
        try {
            String content = Files.readString(specFile.toPath());
            JsonNode spec;
            
            // Try to parse as YAML first, then JSON
            if (specFile.getName().toLowerCase().endsWith(".json")) {
                spec = jsonMapper.readTree(content);
            } else {
                spec = yamlMapper.readTree(content);
            }
            
            // Validate required OpenAPI fields
            if (!spec.has("openapi") && !spec.has("swagger")) {
                result.addError("OpenAPI spec " + specFile.getName() + " is missing 'openapi' or 'swagger' field");
                return false;
            }
            
            if (!spec.has("info")) {
                result.addError("OpenAPI spec " + specFile.getName() + " is missing 'info' section");
                return false;
            }
            
            JsonNode info = spec.get("info");
            if (!info.has("title")) {
                result.addError("OpenAPI spec " + specFile.getName() + " is missing 'info.title'");
                return false;
            }
            
            if (!info.has("version")) {
                result.addError("OpenAPI spec " + specFile.getName() + " is missing 'info.version'");
                return false;
            }
            
            if (!spec.has("paths") || spec.get("paths").isEmpty()) {
                result.addError("OpenAPI spec " + specFile.getName() + " has no paths defined");
                return false;
            }
            
            result.addSuccess("✓ OpenAPI spec " + specFile.getName() + " is valid");
            return true;
            
        } catch (IOException e) {
            result.addError("Failed to read OpenAPI spec " + specFile.getName() + ": " + e.getMessage());
            return false;
        } catch (Exception e) {
            result.addError("Invalid OpenAPI spec " + specFile.getName() + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Result class for validation outcomes.
     */
    public static class ValidationResult {
        private final List<String> errors = new java.util.ArrayList<>();
        private final List<String> warnings = new java.util.ArrayList<>();
        private final List<String> successes = new java.util.ArrayList<>();
        
        public void addError(String error) {
            errors.add(error);
        }
        
        public void addWarning(String warning) {
            warnings.add(warning);
        }
        
        public void addSuccess(String success) {
            successes.add(success);
        }
        
        public List<String> getErrors() {
            return errors;
        }
        
        public List<String> getWarnings() {
            return warnings;
        }
        
        public List<String> getSuccesses() {
            return successes;
        }
        
        public boolean isValid() {
            return errors.isEmpty();
        }
        
        public boolean hasWarnings() {
            return !warnings.isEmpty();
        }
        
        public void logResults(Logger logger) {
            // Print header
            logger.info("=".repeat(60));
            logger.info("🔍 FOUNDATION VALIDATION REPORT");
            logger.info("=".repeat(60));
            
            // Log successes with green checkmarks
            for (String success : successes) {
                logger.info("✅ " + success);
            }
            
            // Log warnings with yellow warning signs
            for (String warning : warnings) {
                logger.warn("⚠️  " + warning);
            }
            
            // Log errors with red X marks
            for (String error : errors) {
                logger.error("❌ " + error);
            }
            
            // Print summary
            logger.info("-".repeat(60));
            if (isValid()) {
                if (hasWarnings()) {
                    logger.info("✅ VALIDATION PASSED with {} warnings", warnings.size());
                    logger.info("💡 Consider addressing the warnings above for optimal performance");
                } else {
                    logger.info("🎉 VALIDATION PASSED - Foundation is ready to use!");
                }
            } else {
                logger.error("❌ VALIDATION FAILED with {} errors", errors.size());
                logger.error("🔧 Please fix the errors above before running the agent");
            }
            logger.info("=".repeat(60));
        }
        
        /**
         * Get a user-friendly summary message for Docker output
         */
        public String getSummaryMessage() {
            if (isValid()) {
                if (hasWarnings()) {
                    return String.format("✅ Foundation validation passed with %d warnings. Consider addressing warnings for optimal performance.", warnings.size());
                } else {
                    return "🎉 Foundation validation passed! Your foundation is ready to use.";
                }
            } else {
                return String.format("❌ Foundation validation failed with %d errors. Please fix the errors before running the agent.", errors.size());
            }
        }
    }
}
