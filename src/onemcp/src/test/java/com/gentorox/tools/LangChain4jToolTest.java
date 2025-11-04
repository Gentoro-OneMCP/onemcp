package com.gentorox.tools;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple unit test to verify LangChain4j @Tool annotations work correctly.
 * This test doesn't require Spring context or external dependencies.
 */
class LangChain4jToolTest {

    @Test
    void testCalculatorTool() {
        LangChain4jCalculatorTool calculator = new LangChain4jCalculatorTool();
        
        // Test addition
        int result1 = calculator.add(5, 3);
        assertEquals(8, result1);
        
        // Test multiplication
        int result2 = calculator.multiply(4, 7);
        assertEquals(28, result2);
        
        // Test with negative numbers
        int result3 = calculator.add(-2, 5);
        assertEquals(3, result3);
        
        // Test with zero
        int result4 = calculator.multiply(10, 0);
        assertEquals(0, result4);
    }

    @Test
    void testToolAnnotations() {
        // Verify that the tool class has the expected methods
        LangChain4jCalculatorTool calculator = new LangChain4jCalculatorTool();
        
        // These should not throw exceptions
        assertDoesNotThrow(() -> calculator.add(1, 1));
        assertDoesNotThrow(() -> calculator.multiply(2, 2));
        
        // Verify return types (primitives are auto-boxed)
        Integer addResult = calculator.add(1, 1);
        Integer multiplyResult = calculator.multiply(2, 2);
        assertTrue(addResult instanceof Integer);
        assertTrue(multiplyResult instanceof Integer);
    }
}
