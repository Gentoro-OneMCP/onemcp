#!/bin/bash

# Demo script showing how to use the InferenceService CLI
# This script demonstrates automated testing of the CLI

echo "=== InferenceService CLI Demo ==="
echo ""

# Check if API keys are set
if [ -z "$OPENAI_API_KEY" ] && [ -z "$GEMINI_API_KEY" ]; then
    echo "❌ No API keys set. Please set at least one API key:"
    echo "  export OPENAI_API_KEY='sk-...'"
    echo "  export GEMINI_API_KEY='AI...'"
    exit 1
fi

echo "API Keys Status:"
echo "  OPENAI_API_KEY: ${OPENAI_API_KEY:+SET}"
echo "  GEMINI_API_KEY: ${GEMINI_API_KEY:+SET}"
echo ""

echo "Starting CLI demo..."
echo ""

# Navigate to the onemcp project directory
cd "$(dirname "$0")/../../packages/server"

# Create input file for automated testing
cat > /tmp/cli_input.txt << EOF
1
What is 2+2? Respond with just the number.
2
What color is the sky? Respond with just the color.
3
1
Calculate 10 * 5
4
1
Tell me a joke
5
EOF

echo "Running automated CLI test..."
echo "Input: OpenAI test, Gemini test, Tool calling test, Custom test, Exit"
echo ""

# Run CLI with automated input
mvn exec:java -Dexec.mainClass="com.gentorox.services.inference.InferenceServiceCLI" -Dexec.classpathScope=test -q < /tmp/cli_input.txt

# Clean up
rm -f /tmp/cli_input.txt

echo ""
echo "Demo complete!"
echo ""
echo "To use the CLI interactively, run:"
echo "  ./scripts/cli/test-inference.sh"
echo ""
echo "Or directly:"
echo "  cd packages/server && mvn exec:java -Dexec.mainClass=\"com.gentorox.services.inference.InferenceServiceCLI\" -Dexec.classpathScope=test"
