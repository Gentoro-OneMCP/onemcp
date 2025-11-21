#!/bin/bash

# Simple script to test the InferenceService CLI
# Make sure to set your API keys as environment variables first

echo "=== InferenceService CLI Test Script ==="
echo ""
echo "Before running, make sure you have set:"
echo "  export OPENAI_API_KEY='your-openai-key'"
echo "  export GEMINI_API_KEY='your-gemini-key'"
echo ""
echo "Current environment variables:"
echo "  OPENAI_API_KEY: ${OPENAI_API_KEY:+SET}"
echo "  GEMINI_API_KEY: ${GEMINI_API_KEY:+SET}"
echo ""

if [ -z "$OPENAI_API_KEY" ] && [ -z "$GEMINI_API_KEY" ]; then
    echo "❌ No API keys set. Please set at least one API key."
    echo ""
    echo "Example:"
    echo "  export OPENAI_API_KEY='sk-...'"
    echo "  export GEMINI_API_KEY='AI...'"
    echo "  ./scripts/cli/test-inference.sh"
    exit 1
fi

echo "Starting InferenceService CLI..."
echo ""

# Navigate to the onemcp project directory
cd "$(dirname "$0")/../../packages/server"
mvn exec:java -Dexec.mainClass="com.gentorox.services.inference.InferenceServiceCLI" -Dexec.classpathScope=test
