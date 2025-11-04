package com.gentorox.services.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FoundationValidationServiceTest {

    @TempDir
    Path tempDir;
    
    private FoundationValidationService validationService;
    
    @BeforeEach
    void setUp() {
        validationService = new FoundationValidationService();
    }
    
    @Test
    void testValidateFoundation_MissingDirectory() {
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation("/nonexistent/directory");
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
            .anyMatch(error -> error.contains("Foundation directory not found")));
    }
    
    @Test
    void testValidateFoundation_MissingAgentMd() throws IOException {
        // Create foundation directory without Agent.md
        Files.createDirectories(tempDir);
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
            .anyMatch(error -> error.contains("Agent.md is missing")));
    }
    
    @Test
    void testValidateFoundation_EmptyAgentMd() throws IOException {
        // Create foundation directory with empty Agent.md
        Files.createDirectories(tempDir);
        Files.createFile(tempDir.resolve("Agent.md"));
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
            .anyMatch(error -> error.contains("Agent.md is empty")));
    }
    
    @Test
    void testValidateFoundation_ValidAgentMd_NoDocs() throws IOException {
        // Create foundation directory with valid Agent.md but no docs
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("Agent.md"), createValidAgentMd());
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
            .anyMatch(error -> error.contains("docs/ directory is missing")));
    }
    
    @Test
    void testValidateFoundation_ValidAgentMd_NoOpenApi() throws IOException {
        // Create foundation directory with valid Agent.md and docs but no OpenAPI
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("Agent.md"), createValidAgentMd());
        
        Files.createDirectories(tempDir.resolve("docs"));
        Files.writeString(tempDir.resolve("docs/README.md"), "# Documentation");
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
            .anyMatch(error -> error.contains("apis/ directory is missing")));
    }
    
    @Test
    void testValidateFoundation_CompleteValidFoundation() throws IOException {
        // Create complete valid foundation
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("Agent.md"), createValidAgentMd());
        
        Files.createDirectories(tempDir.resolve("docs"));
        Files.writeString(tempDir.resolve("docs/README.md"), "# Documentation");
        
        Files.createDirectories(tempDir.resolve("apis"));
        Files.writeString(tempDir.resolve("apis/api.yaml"), createValidOpenApiSpec());
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertTrue(result.isValid());
        assertTrue(result.getSuccesses().stream()
            .anyMatch(success -> success.contains("Agent.md found and validated")));
        assertTrue(result.getSuccesses().stream()
            .anyMatch(success -> success.contains("Documentation found")));
        assertTrue(result.getSuccesses().stream()
            .anyMatch(success -> success.contains("Valid OpenAPI specification found")));
    }
    
    @Test
    void testValidateFoundation_InvalidOpenApiSpec() throws IOException {
        // Create foundation with invalid OpenAPI spec
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("Agent.md"), createValidAgentMd());
        
        Files.createDirectories(tempDir.resolve("docs"));
        Files.writeString(tempDir.resolve("docs/README.md"), "# Documentation");
        
        Files.createDirectories(tempDir.resolve("apis"));
        Files.writeString(tempDir.resolve("apis/invalid.yaml"), "invalid: yaml: content");
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
            .anyMatch(error -> error.contains("No valid OpenAPI specification found")));
    }
    
    @Test
    void testValidateFoundation_AgentMdFormatWarnings() throws IOException {
        // Create foundation with minimal Agent.md (should generate warnings)
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("Agent.md"), "# Agent\nShort content");
        
        Files.createDirectories(tempDir.resolve("docs"));
        Files.writeString(tempDir.resolve("docs/README.md"), "# Documentation");
        
        Files.createDirectories(tempDir.resolve("apis"));
        Files.writeString(tempDir.resolve("apis/api.yaml"), createValidOpenApiSpec());
        
        FoundationValidationService.ValidationResult result = 
            validationService.validateFoundation(tempDir.toString());
        
        assertTrue(result.isValid());
        assertTrue(result.hasWarnings());
        assertTrue(result.getWarnings().stream()
            .anyMatch(warning -> warning.contains("Agent.md should describe the agent's purpose")));
    }
    
    private String createValidAgentMd() {
        return """
            # ACME Sales Analytics Agent
            
            ## Purpose
            This agent helps users query and understand ACME e-commerce sales data using the ACME Sales Analytics API.
            
            ## Behavioral Guidance
            - Be concise and factual
            - Use the API/SDKs and knowledge base
            - Validate fields against the spec before using them
            
            ## Tools
            - RetrieveContext: Retrieve KB resources
            - RunTypescriptSnippet: Execute TypeScript code
            
            ## Safety & Scope
            - Operate only within the ACME Sales Analytics API
            - Do not fabricate data
            - Never expose secrets
            """;
    }
    
    private String createValidOpenApiSpec() {
        return """
            openapi: 3.0.0
            info:
              title: ACME Sales Analytics API
              version: 1.0.0
              description: API for ACME sales analytics
            paths:
              /sales:
                get:
                  summary: Get sales data
                  responses:
                    '200':
                      description: Successful response
                      content:
                        application/json:
                          schema:
                            type: object
            """;
    }
}
