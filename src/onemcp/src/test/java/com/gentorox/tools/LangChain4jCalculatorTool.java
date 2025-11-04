package com.gentorox.tools;

import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.agent.tool.P;
import org.springframework.stereotype.Component;

/**
 * Example of a proper LangChain4j tool using @Tool and @P annotations.
 * This demonstrates how tools should be defined in LangChain4j.
 */
@Component
public class LangChain4jCalculatorTool {

    @Tool("Adds two numbers and returns the result")
    public int add(@P("The first number") int a, @P("The second number") int b) {
        return a + b;
    }

    @Tool("Subtracts the second number from the first number")
    public int subtract(@P("The first number") int a, @P("The second number") int b) {
        return a - b;
    }

    @Tool("Multiplies two numbers and returns the result")
    public int multiply(@P("The first number") int a, @P("The second number") int b) {
        return a * b;
    }

    @Tool("Divides the first number by the second number")
    public double divide(@P("The dividend") double a, @P("The divisor") double b) {
        if (b == 0) {
            throw new IllegalArgumentException("Cannot divide by zero");
        }
        return a / b;
    }

    @Tool("Calculates the square root of a number")
    public double sqrt(@P("The number to calculate square root of") double number) {
        if (number < 0) {
            throw new IllegalArgumentException("Cannot calculate square root of negative number");
        }
        return Math.sqrt(number);
    }
}
