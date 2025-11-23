package com.gentoro.onemcp.indexing.driver.arangodb;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.gentoro.onemcp.indexing.graph.GraphEdge;
import com.gentoro.onemcp.indexing.graph.nodes.EntityNode;
import java.util.ArrayList;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ArangoIndexingDriverTest {

  @Mock private com.gentoro.onemcp.OneMcp oneMcp;
  @Mock private Configuration configuration;

  private ArangoIndexingDriver arangoIndexingDriver;

  @BeforeEach
  void setUp() {
    // Use lenient mocking to avoid issues with OneMcp class
    lenient().when(oneMcp.configuration()).thenReturn(configuration);
    arangoIndexingDriver = new ArangoIndexingDriver(oneMcp, "test_handbook");
  }

  @Test
  void testConstructor() {
    ArangoIndexingDriver driver = new ArangoIndexingDriver(oneMcp, "test_handbook");
    assertNotNull(driver);
  }

  @Test
  void testInitializeReadsConfiguration() {
    // Arrange - verify configuration is read with correct defaults
    when(configuration.getString("graph.indexing.arangodb.host", "localhost"))
        .thenReturn("localhost");
    when(configuration.getInteger("graph.indexing.arangodb.port", 8529)).thenReturn(8529);
    when(configuration.getString("graph.indexing.arangodb.user", "root")).thenReturn("root");
    when(configuration.getString("graph.indexing.arangodb.password", "")).thenReturn("");
    when(configuration.getBoolean("graph.indexing.arangodb.enabled", false)).thenReturn(false);

    // Act - initialization should skip when disabled
    arangoIndexingDriver.initialize();

    // Assert - driver should not be initialized
    assertFalse(arangoIndexingDriver.isInitialized());
  }

  @Test
  void testStoreNodeSkipsWhenNotInitialized() {
    // Arrange
    EntityNode node =
        new EntityNode("test-key", "Test Entity", "Description", "test-service", new ArrayList<>());

    // Act & Assert - should not throw, just log warning
    assertDoesNotThrow(() -> arangoIndexingDriver.storeNode(node));
  }

  @Test
  void testStoreEdgeSkipsWhenNotInitialized() {
    // Arrange
    GraphEdge edge = new GraphEdge("from-key", "to-key", "HAS_OPERATION");

    // Act & Assert - should not throw, just log warning
    assertDoesNotThrow(() -> arangoIndexingDriver.storeEdge(edge));
  }

  @Test
  void testShutdownWhenNotInitialized() {
    // Act & Assert - should not throw
    assertDoesNotThrow(() -> arangoIndexingDriver.shutdown());
  }

  @Test
  void testGetDatabaseReturnsNullWhenNotInitialized() {
    // Act
    var result = arangoIndexingDriver.getDatabase();

    // Assert
    assertNull(result);
  }
}
