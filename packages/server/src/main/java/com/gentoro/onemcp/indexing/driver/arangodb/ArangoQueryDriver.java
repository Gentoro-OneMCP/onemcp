package com.gentoro.onemcp.indexing.driver.arangodb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.indexing.driver.GraphQueryDriver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ArangoDB implementation of {@link GraphQueryDriver}.
 *
 * <p>Encapsulates the optimized AQL traversal used to fetch entity context (operations, fields,
 * documentation, etc.) from the graph.
 */
public class ArangoQueryDriver implements GraphQueryDriver {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(ArangoQueryDriver.class);

  private static final String CONFIG_PREFIX = "graph.query.arangodb.";

  private final ArangoIndexingDriver arangoIndexingDriver;
  private final String entityContextQueryPath;
  private volatile String entityContextQueryTemplate;

  /**
   * Create a driver using an existing {@link ArangoIndexingDriver} instance.
   *
   * @param oneMcp application context for configuration
   * @param arangoIndexingDriver initialized (or initializable) indexing driver
   */
  public ArangoQueryDriver(OneMcp oneMcp, ArangoIndexingDriver arangoIndexingDriver) {
    this.arangoIndexingDriver = arangoIndexingDriver;
    this.entityContextQueryPath =
        oneMcp
            .configuration()
            .getString(CONFIG_PREFIX + "entityContextQueryPath", "/aql/entity-context-query.aql");
  }

  /**
   * Create a driver from {@link OneMcp} context and handbook name.
   *
   * @param oneMcp application context
   * @param handbookName handbook identifier
   */
  public ArangoQueryDriver(OneMcp oneMcp, String handbookName) {
    this(oneMcp, new ArangoIndexingDriver(oneMcp, handbookName));
  }

  @Override
  public void initialize() {
    arangoIndexingDriver.initialize();
    loadQueryTemplates();
  }

  @Override
  public boolean isInitialized() {
    return arangoIndexingDriver.isInitialized();
  }

  @Override
  public Map<String, Object> queryContext(String entityName, List<String> requestedCategories) {
    if (!arangoIndexingDriver.isInitialized()) {
      throw new IoException("ArangoDB service is not initialized");
    }

    String effectiveEntity = entityName != null ? entityName : "";
    List<String> categories =
        (requestedCategories == null || requestedCategories.isEmpty())
            ? Collections.emptyList()
            : requestedCategories;

    String aql = entityContextQueryTemplate;
    if (aql == null) {
      throw new IoException("AQL query template not loaded. Ensure initialize() was called.");
    }

    ArangoDatabase db = arangoIndexingDriver.getDatabase();
    Map<String, Object> bindVars = new HashMap<>();
    bindVars.put("entityName", effectiveEntity);
    // Build entity key in the same format as stored: entity|EntityName -> entity_EntityName
    String entityKey = buildEntityKey(effectiveEntity);
    bindVars.put("entityKey", entityKey);
    bindVars.put("categories", categories);

    log.debug("Executing Arango AQL query for entity: {}", effectiveEntity);
    log.debug("Entity key: {}", entityKey);
    log.debug("Bind vars: {}", bindVars);
    log.trace("AQL query:\n{}", aql);

    try {
      @SuppressWarnings({"unchecked", "rawtypes"})
      ArangoCursor<Map<String, Object>> cursor =
          (ArangoCursor) db.query(aql, Map.class, bindVars, null);

      if (!cursor.hasNext()) {
        return null;
      }

      return cursor.next();

    } catch (Exception e) {
      log.error("Error executing Arango graph query for entity: {}", effectiveEntity, e);
      throw new IoException("Graph query failed for entity: " + effectiveEntity, e);
    }
  }

  @Override
  public String getDriverName() {
    return "arangodb";
  }

  @Override
  public String getHandbookName() {
    return arangoIndexingDriver.getHandbookName();
  }

  @Override
  public void shutdown() {
    arangoIndexingDriver.shutdown();
  }

  /**
   * Load AQL query templates from classpath resources.
   *
   * <p>Lazily loads the entity context query template from the configured path. The template is
   * cached after first load for performance.
   *
   * @throws IoException if the template cannot be loaded
   */
  private void loadQueryTemplates() {
    if (entityContextQueryTemplate == null) {
      try (InputStream is = getClass().getResourceAsStream(entityContextQueryPath)) {
        if (is == null) {
          throw new IoException("AQL query template not found: " + entityContextQueryPath);
        }
        entityContextQueryTemplate = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        log.debug("Loaded AQL query template from: {}", entityContextQueryPath);
      } catch (IOException e) {
        throw new IoException("Failed to load AQL query template: " + entityContextQueryPath, e);
      }
    }
  }

  /**
   * Build the entity key in the same format as stored in ArangoDB.
   *
   * <p>Entities are stored with keys like "entity|Sale" which get sanitized to "entity_Sale". This
   * method replicates the same sanitization logic used by ArangoIndexingDriver.
   *
   * @param entityName the entity name (e.g., "Sale")
   * @return sanitized entity key (e.g., "entity_Sale")
   */
  private String buildEntityKey(String entityName) {
    if (entityName == null || entityName.isBlank()) {
      return "";
    }
    // Build key in the same format as stored: entity|EntityName
    String originalKey = "entity|" + entityName;
    // Use the same sanitization as ArangoIndexingDriver.sanitizeKeyForArangoDB
    // Replace | with _ and preserve case
    return originalKey.replace("|", "_");
  }
}
