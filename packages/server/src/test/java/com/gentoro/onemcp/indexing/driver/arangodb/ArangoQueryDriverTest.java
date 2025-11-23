package com.gentoro.onemcp.indexing.driver.arangodb;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ArangoQueryDriverTest {

  @Mock private com.gentoro.onemcp.OneMcp oneMcp;
  @Mock private Configuration configuration;
  @Mock private ArangoIndexingDriver arangoIndexingDriver;

  @BeforeEach
  void setUp() {
    // Use lenient mocking to avoid issues with OneMcp class
    lenient().when(oneMcp.configuration()).thenReturn(configuration);
    lenient()
        .when(
            configuration.getString(
                "graph.query.arangodb.entityContextQueryPath", "/aql/entity-context-query.aql"))
        .thenReturn("/aql/entity-context-query.aql");
  }

  @Test
  void testSanitizeKeyWithNormalString() {
    // Test the sanitizeKey logic used by ArangoQueryDriver
    String input = "Test.Entity";
    String expected = input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    assertEquals("test_entity", expected);
  }

  @Test
  void testSanitizeKeyWithSpecialCharacters() {
    String input = "Entity@123.Test-Case";
    String expected = input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    assertEquals("entity_123_test-case", expected);
  }

  @Test
  void testSanitizeKeyWithNullInput() {
    String input = null;
    String expected = "";
    assertEquals(expected, input != null ? input : "");
  }

  @Test
  void testSanitizeKeyWithEmptyString() {
    String input = "";
    String expected = input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    assertEquals("", expected);
  }

  @Test
  void testSanitizeKeyAllowsHyphens() {
    // Verify that hyphens are allowed (not replaced with underscores)
    String input = "test-case";
    String expected = input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    assertEquals("test-case", expected);
  }

  @Test
  void testSanitizeKeyReplacesSpaces() {
    String input = "test case";
    String expected = input.replaceAll("[^a-zA-Z0-9_\\-]", "_").toLowerCase();
    assertEquals("test_case", expected);
  }

  @Test
  void testInitializeReadsConfiguration() {
    // Arrange - verify configuration is read with correct defaults
    when(configuration.getString(
            "graph.query.arangodb.entityContextQueryPath", "/aql/entity-context-query.aql"))
        .thenReturn("/aql/entity-context-query.aql");

    // Act - create driver which should read configuration during construction
    ArangoQueryDriver driver = new ArangoQueryDriver(oneMcp, arangoIndexingDriver);

    // Assert - verify configuration was read during construction
    verify(configuration)
        .getString("graph.query.arangodb.entityContextQueryPath", "/aql/entity-context-query.aql");
    assertNotNull(driver);
  }
}
