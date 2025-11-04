# InferenceService Tests

This directory contains test cases for the InferenceService that demonstrate how to load API keys from environment variables and test the inference API.

## Test Files

### 1. `InferenceServiceManualTest.java`
A manual test class that can be run directly to test the InferenceService without requiring Spring Boot context.

### 2. `SimpleInferenceTest.java`
JUnit test cases that test the InferenceService with different providers.

### 3. `ProviderConfigTest.java`
Test cases for the ProviderConfig class to verify environment variable loading.

## Running the Tests

### Prerequisites

Set at least one of the following environment variables:

```bash
export OPENAI_API_KEY="your-openai-api-key"
export ANTHROPIC_API_KEY="your-anthropic-api-key"
export GEMINI_API_KEY="your-gemini-api-key"
```

### Interactive CLI (Recommended)

The easiest way to test the InferenceService is using the interactive CLI:

```bash
# Using the convenience script (from project root)
cd onemcp
./scripts/cli/test-inference.sh

# Or directly with Maven (from onemcp directory)
cd onemcp/src/onemcp
mvn exec:java -Dexec.mainClass="com.gentorox.services.inference.InferenceServiceCLI" -Dexec.classpathScope=test
```

The CLI provides an interactive menu to:
- Test OpenAI inference
- Test Gemini inference  
- Test tool calling functionality
- Run custom tests with your own prompts

### Manual Test (Non-interactive)

For automated testing without user interaction:

```bash
# Compile the project
cd onemcp/src/onemcp
mvn compile -Dmaven.test.skip=true

# Run the manual test
mvn exec:java -Dexec.mainClass="com.gentorox.services.inference.InferenceServiceManualTest" -Dexec.classpathScope=test
```

### JUnit Tests

If you want to run the JUnit tests (may fail due to compilation issues):

```bash
# Run specific test
mvn test -Dtest=SimpleInferenceTest#testEnvironmentVariables

# Run all inference tests
mvn test -Dtest="*Inference*"
```

## What the Tests Verify

1. **Environment Variable Loading**: Confirms that API keys are loaded from environment variables
2. **Provider Configuration**: Tests that different providers (OpenAI, Anthropic, Gemini) can be configured
3. **Basic Inference**: Tests simple text generation requests
4. **Tool Calling**: Tests native tool calling functionality with LangChain4j
5. **Error Handling**: Tests error cases like missing API keys or unknown providers

## Test Output

The manual test will output:
- Environment variable status
- Test results for each available provider
- Tool calling test results
- Any errors encountered

## Configuration

The tests use the following model configurations for cost efficiency:
- OpenAI: `gpt-4o-mini`
- Anthropic: `claude-3-haiku-20240307`
- Gemini: `gemini-2.0-flash`

## Troubleshooting

### Compilation Issues
If you encounter MCP-related compilation errors, use the manual test instead of JUnit tests.

### Missing API Keys
Set the appropriate environment variables before running tests.

### Network Issues
Ensure your network allows outbound connections to the AI provider APIs.

### Rate Limits
The tests use minimal requests, but be aware of API rate limits.
