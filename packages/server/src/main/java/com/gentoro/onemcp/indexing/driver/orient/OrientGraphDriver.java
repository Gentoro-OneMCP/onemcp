package com.gentoro.onemcp.indexing.driver.orient;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.HandbookException;
import com.gentoro.onemcp.exception.IoException;
import com.gentoro.onemcp.indexing.GraphContextTuple;
import com.gentoro.onemcp.indexing.GraphDriver;
import com.gentoro.onemcp.indexing.GraphNodeRecord;
import com.gentoro.onemcp.utility.FileUtility;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrientGraphDriver implements GraphDriver {

  private final OneMcp oneMcp;
  private final String handbookName;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private OrientDB orient;
  private ODatabaseSession db;

  private String database;
  private File dbRoot;

  public OrientGraphDriver(OneMcp oneMcp, String handbookName) {
    this.oneMcp = Objects.requireNonNull(oneMcp, "oneMcp");
    this.handbookName = handbookName != null ? handbookName : "default";
  }

  @Override
  public void initialize() {
    if (initialized.get()) return;

    try {
      String root = oneMcp.configuration().getString("graph.orient.rootDir", "data/orient");
      String prefix = oneMcp.configuration().getString("graph.orient.databasePrefix", "onemcp-");
      String configured = oneMcp.configuration().getString("graph.orient.database", "");
      this.database =
          (configured != null && !configured.isBlank()) ? configured : (prefix + handbookName);

      File storageRoot = Path.of(root).toFile();
      if (!storageRoot.exists() && !storageRoot.mkdirs()) {
        throw new IoException("Unable to create OrientDB storage root directory: " + storageRoot);
      }

      // CORRECT: actual DB folder in embedded mode
      this.dbRoot = Path.of(root, "databases", this.database).toFile();

      if (oneMcp.configuration().getBoolean("graph.indexing.clearOnStartup", false)) {
        FileUtility.deleteDir(dbRoot.toPath(), true);
      }

      String url = "embedded:" + storageRoot.getAbsolutePath();
      orient =
          new OrientDB(
              url,
              "admin",
              "admin",
              OrientDBConfig.builder().addGlobalUser("admin", "admin", "*").build());

      if (!orient.exists(database)) {
        orient.create(database, ODatabaseType.PLOCAL);

        // IMPORTANT: Force initialization of security + metadata
        try (ODatabaseSession initDb = orient.open(database, "admin", "admin")) {
          // DB bootstrap completes here
        }
      }

      // admin/admin ALWAYS works when DB folder is correct
      db = orient.open(database, "admin", "admin");

      setupSchema();
      initialized.set(true);

    } catch (Exception e) {
      throw new HandbookException("Failed to initialize OrientGraphDriver", e);
    }
  }

  private void createPropertyIfNotExists(OClass entity, String name, OType type) {
    if (!entity.existsProperty(name)) {
      entity.createProperty(name, type);
    }
  }

  private OClass ensureVertexClass(OSchema schema, String name) {
    OClass cls = schema.getClass(name);
    if (cls == null) {
      cls = schema.createClass(name, schema.getClass("V"));
    }
    return cls;
  }

  private OClass ensureEdgeClass(OSchema schema, String name) {
    OClass cls = schema.getClass(name);
    if (cls == null) {
      cls = schema.createClass(name, schema.getClass("E"));
    }
    return cls;
  }

  private void ensureProperty(OClass cls, String prop, OType type) {
    if (cls.getProperty(prop) == null) {
      cls.createProperty(prop, type);
    }
  }

  private void ensureUniqueIndex(OClass cls, String indexName, String prop) {
    if (cls.getClassIndex(indexName) == null) {
      cls.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, prop);
    }
  }

  private void ensureNotUniqueIndex(OClass cls, String indexName, String prop) {
    if (cls.getClassIndex(indexName) == null) {
      cls.createIndex(indexName, OClass.INDEX_TYPE.NOTUNIQUE, prop);
    }
  }

  private void setupSchema() {
    OSchema schema = db.getMetadata().getSchema();

    // === Vertex Classes ===
    OClass kn = ensureVertexClass(schema, "KnowledgeNode");
    OClass entity = ensureVertexClass(schema, "Entity");
    OClass op = ensureVertexClass(schema, "Operation");

    // === Properties ===
    ensureProperty(entity, "name", OType.STRING);
    ensureProperty(op, "name", OType.STRING);

    ensureUniqueIndex(entity, "Entity.name.unique", "name");
    ensureUniqueIndex(op, "Operation.name.unique", "name");

    ensureProperty(kn, "key", OType.STRING);
    ensureProperty(kn, "nodeType", OType.STRING);
    ensureProperty(kn, "apiSlug", OType.STRING);
    ensureProperty(kn, "operationId", OType.STRING);
    ensureProperty(kn, "content", OType.STRING);
    ensureProperty(kn, "contentFormat", OType.STRING);
    ensureProperty(kn, "docPath", OType.STRING);
    ensureProperty(kn, "title", OType.STRING);
    ensureProperty(kn, "summary", OType.STRING);

    ensureUniqueIndex(kn, "KnowledgeNode.key.unique", "key");

    // === Edges ===
    ensureEdgeClass(schema, "HasEntity");
    ensureEdgeClass(schema, "HasOperation");
  }

  @Override
  public boolean isInitialized() {
    return initialized.get();
  }

  @Override
  public void upsertNodes(List<GraphNodeRecord> nodes) {
    if (nodes == null || nodes.isEmpty()) return;

    for (GraphNodeRecord rec : nodes) {
      Map<String, Object> m = rec.toMap();

      String key = (String) m.get("key");

      OResult existing =
          db.query("SELECT FROM KnowledgeNode WHERE key = ?", key).stream()
              .findFirst()
              .orElse(null);

      ODocument node =
          existing != null ? (ODocument) existing.toElement() : new ODocument("KnowledgeNode");

      node.field("key", key);
      node.field("nodeType", m.get("nodeType"));
      node.field("apiSlug", m.get("apiSlug"));
      node.field("operationId", m.get("operationId"));
      node.field("content", m.get("content"));
      node.field("contentFormat", m.get("contentFormat"));
      node.field("docPath", m.get("docPath"));
      node.field("title", m.get("title"));
      node.field("summary", m.get("summary"));

      node.save();

      db.command("DELETE EDGE HasEntity WHERE out = ?", node.getIdentity());
      db.command("DELETE EDGE HasOperation WHERE out = ?", node.getIdentity());

      // Support both v2 canonical field "entities" (list) and legacy single "entity"
      Object ents = m.get("entities");
      if (ents instanceof Collection<?> c) {
        for (Object eo : c) {
          String en = Objects.toString(eo, null);
          if (en == null || en.isBlank()) continue;
          ODocument e = getOrCreateVertex("Entity", "name", en);
          createEdge(node, e, "HasEntity");
        }
      } else {
        String entity = (String) m.get("entity");
        if (entity != null) {
          ODocument e = getOrCreateVertex("Entity", "name", entity);
          createEdge(node, e, "HasEntity");
        }
      }

      @SuppressWarnings("unchecked")
      List<String> ops = (List<String>) m.getOrDefault("operations", List.of());
      for (String opName : ops) {
        ODocument op = getOrCreateVertex("Operation", "name", opName);
        createEdge(node, op, "HasOperation");
      }
    }
  }

  private ODocument getOrCreateVertex(String cls, String field, String value) {
    OResult r =
        db.query("SELECT FROM " + cls + " WHERE " + field + " = ?", value).stream()
            .findFirst()
            .orElse(null);
    if (r != null) return (ODocument) r.toElement();

    ODocument doc = new ODocument(cls);
    doc.field(field, value);
    doc.save();
    return doc;
  }

  private void createEdge(ODocument out, ODocument in, String edgeClass) {
    db.command("CREATE EDGE " + edgeClass + " FROM ? TO ?", out.getIdentity(), in.getIdentity());
  }

  @Override
  public List<Map<String, Object>> queryByContext(List<GraphContextTuple> tuples) {
    if (tuples == null || tuples.isEmpty()) {
      List<Map<String, Object>> all = new ArrayList<>();
      for (OResult r : db.query("SELECT FROM KnowledgeNode").stream().toList()) {
        all.add(((ODocument) r.toElement()).toMap());
      }
      return all;
    }

    Map<String, Set<String>> opsByEntity = new HashMap<>();
    Set<String> allEntities = new HashSet<>();

    for (GraphContextTuple t : tuples) {
      allEntities.add(t.getEntity());
      opsByEntity.computeIfAbsent(t.getEntity(), k -> new HashSet<>()).addAll(t.getOperations());
    }

    List<Map<String, Object>> results = new ArrayList<>();

    for (String entity : allEntities) {
      String sql =
          """
                SELECT expand(kn) FROM (
                  MATCH {
                      class: Entity, as:e, where:(name = :entity)
                  }.in('HasEntity') {
                      as: kn
                  }
                  RETURN DISTINCT kn
                )
            """;

      List<ODocument> knodes = new ArrayList<>();
      for (OResult r : db.query(sql, Map.of("entity", entity)).stream().toList()) {
        // After expand(kn), results should be full records; still guard and load if needed
        OElement el = r.toElement();
        if (el instanceof ODocument doc) {
          knodes.add(doc);
        } else if (r.getIdentity().isPresent()) {
          knodes.add(db.load(r.getIdentity().get()));
        }
      }

      Set<String> requestedOps = opsByEntity.getOrDefault(entity, Set.of());

      for (ODocument kn : knodes) {

        List<String> nodeOps = new ArrayList<>();
        for (OResult r :
            db
                .query(
                    "SELECT name FROM (SELECT expand(out('HasOperation')) FROM ?)",
                    kn.getIdentity())
                .stream()
                .toList()) {
          nodeOps.add(r.getProperty("name"));
        }

        if (requestedOps.isEmpty() || nodeOps.isEmpty()) {
          results.add(kn.toMap());
          continue;
        }

        boolean ok = nodeOps.stream().anyMatch(requestedOps::contains);
        if (ok) results.add(kn.toMap());
      }
    }

    return results;
  }

  @Override
  public void deleteNodesByKeys(List<String> keys) {
    db.command("DELETE FROM KnowledgeNode WHERE key IN ?", keys);
  }

  @Override
  public void clearAll() {
    db.command("DELETE VERTEX KnowledgeNode");
    db.command("DELETE VERTEX Entity");
    db.command("DELETE VERTEX Operation");
  }

  @Override
  public void shutdown() {
    initialized.set(false);
    try {
      if (db != null) db.close();
      if (orient != null) orient.close();
    } catch (Exception ignore) {
    }
  }

  @Override
  public String getDriverName() {
    return "orientdb";
  }

  @Override
  public String getHandbookName() {
    return handbookName;
  }
}
