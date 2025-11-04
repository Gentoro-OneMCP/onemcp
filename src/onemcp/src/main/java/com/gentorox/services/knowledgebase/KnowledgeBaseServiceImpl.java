package com.gentorox.services.knowledgebase;

import com.gentorox.core.model.InferenceResponse;
import com.gentorox.services.inference.InferenceService;
import com.gentorox.services.typescript.TypescriptRuntimeClient;
import com.gentorox.services.typescript.TypescriptRuntimeClient.DocsResponse;
import com.gentorox.services.typescript.TypescriptRuntimeClient.DocFile;
import com.gentorox.services.typescript.TypescriptRuntimeClient.UploadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default KnowledgeBaseService implementation.
 *
 * Features:
 * - Scans a Foundation directory for docs (docs/*.md), tests (tests/**), feedback, and OpenAPI specs.
 * - Produces abstract kb:// URIs mapped to original file:// or in-memory mem:// resources.
 * - Optionally generates concise hints using the configured InferenceService (or falls back to first bytes).
 * - Persists a lightweight KnowledgeBaseState so startup can restore from cache when nothing changed.
 */
/**
 * Default implementation of {@link KnowledgeBaseService} that builds a lightweight searchable catalog of
 * foundation documents, tests, feedback, and generated OpenAPI docs. It persists a compact state to speed up
 * subsequent startups by restoring from cache when no changes are detected.
 */
public class KnowledgeBaseServiceImpl implements KnowledgeBaseService {
  private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseServiceImpl.class);
  private final InferenceService inferenceService;
  private final TypescriptRuntimeClient tsRuntimeClient;
  private final KnowledgeBasePersistence persistence;

  // Entries are keyed by abstracted kb:// URIs
  private final Map<String, KnowledgeBaseEntry> entries = new ConcurrentHashMap<>();
  // Map abstract kb:// -> original (file:// or mem://) for content resolution
  private final Map<String, String> abstractToOriginal = new ConcurrentHashMap<>();
  // In-memory content for mem:// resources
  private final Map<String, String> inMemoryContent = new ConcurrentHashMap<>();
  private boolean loadedFromCache = false;
  private Path stateFile;
  private Path foundationRoot;
  private final boolean aiHintGenerationEnabled;
  private final int hintContentLimit;

  private final Map<String, String> compiledSDKs = new ConcurrentHashMap<>();

  /**
   * Creates a new service instance.
   *
   * @param inferenceService service used to optionally generate human-friendly hints
   * @param tsRuntimeClient Typescript runtime client used for OpenAPI SDK generation and docs fetching
   * @param persistence persistence component used to store/load a compact KB state
   * @param stateFile file where the KB state is persisted; if null, a default under target/kb is used
   * @param aiHintGenerationEnabled whether AI-based hint generation is enabled
   * @param hintContentLimit maximum hint length in characters
   */
  public KnowledgeBaseServiceImpl(InferenceService inferenceService,
                                  TypescriptRuntimeClient tsRuntimeClient,
                                  KnowledgeBasePersistence persistence,
                                  Path stateFile,
                                  boolean aiHintGenerationEnabled,
                                  int hintContentLimit) {
    this.inferenceService = Objects.requireNonNull(inferenceService, "inferenceService");
    this.tsRuntimeClient = Objects.requireNonNull(tsRuntimeClient, "tsRuntimeClient");
    this.persistence = Objects.requireNonNull(persistence, "persistence");
    this.stateFile = (stateFile != null) ? stateFile : Path.of("target/kb/knowledge-base-state.json");
    this.aiHintGenerationEnabled = aiHintGenerationEnabled;
    this.hintContentLimit = hintContentLimit;
  }

  public KnowledgeBaseServiceImpl(InferenceService inferenceService,
                                  TypescriptRuntimeClient tsRuntimeClient,
                                  KnowledgeBasePersistence persistence) {
    this(inferenceService, tsRuntimeClient, persistence,
        Path.of("target/kb/knowledge-base-state.json"), false, 240);
  }

  /**
   * Initializes the knowledge base by attempting to restore from the persisted state file. If the state
   * is missing or stale (signature mismatch), performs a fresh scan of the foundation directory and then
   * persists the new state.
   *
   * @param foundationRoot the root directory containing docs, tests, feedback, and openapi; if null, defaults to "foundation"
   */
  @Override
  public void initialize(Path foundationRoot) {
    if (foundationRoot == null) foundationRoot = Path.of("foundation");
    this.foundationRoot = foundationRoot.toAbsolutePath().normalize();

    String signature = computeSignature(foundationRoot);
    try {
      Optional<KnowledgeBaseState> cached = persistence.load(stateFile);
      if (cached.isPresent() && Objects.equals(cached.get().signature(), signature)) {
        entries.clear();
        abstractToOriginal.clear();
        inMemoryContent.clear();
        for (KnowledgeBaseEntry e : cached.get().entries()) {
          // Entries are stored with abstract URIs
          entries.put(e.resource(), e);
        }
        compiledSDKs.putAll(cached.get().services());
        loadedFromCache = true;
        return;
      }
    } catch (IOException e) { logger.warn("Failed to load KnowledgeBaseState from {}", stateFile, e); }

    // Fresh build
    compiledSDKs.clear();
    entries.clear();
    abstractToOriginal.clear();
    inMemoryContent.clear();
    loadedFromCache = false;

    ingestFoundation(foundationRoot);

    // Persist state
    try {
      var state = new KnowledgeBaseState(signature,
          new ArrayList<>(entries.values()),
          compiledSDKs);
      persistence.save(stateFile, state);
    } catch (IOException e) { logger.warn("Failed to persist KnowledgeBaseState to {}", stateFile, e); }
  }

  @Override
  public Optional<Map<String, String>> getServices() {
    return Optional.of( Map.copyOf(compiledSDKs) );
  }

  /**
   * Lists knowledge base entries under the provided abstract directory prefix.
   * If the prefix is null or blank, all entries are returned.
   *
   * @param dirPrefix a prefix like "kb://docs" or "kb://openapi"; may be null/blank
   * @return sorted list of entries by resource URI
   */
  @Override
  public List<KnowledgeBaseEntry> list(String dirPrefix) {
    if (dirPrefix == null || dirPrefix.isBlank()) return entries.values().stream()
        .sorted(Comparator.comparing(KnowledgeBaseEntry::resource))
        .toList();
    String prefix = dirPrefix;
    return entries.values().stream()
        .filter(e -> e.resource().startsWith(prefix))
        .sorted(Comparator.comparing(KnowledgeBaseEntry::resource))
        .toList();
  }

  /**
   * Resolves and returns the full textual content for a given resource URI.
   * Supports indirection from abstract kb:// URIs to original file:// or in-memory mem:// locations.
   *
   * @param resourceUri the abstract or original URI
   * @return the content if available
   */
  @Override
  public Optional<String> getContent(String resourceUri) {
    try {
      if( entries.containsKey(resourceUri) ) {
        return Optional.of(entries.get(resourceUri).content());
      }
      // If the given URI is abstract, resolve to original
      String original = abstractToOriginal.getOrDefault(resourceUri, null);

      if (original == null && resourceUri != null && resourceUri.startsWith("kb://")) {
        original = resolveOriginalFromAbstract(resourceUri).orElse(null);
      }

      if (original == null) return Optional.empty();

      // Check in-memory bucket for mem:// originals
      if (original.startsWith("mem://")) {
        return Optional.ofNullable(inMemoryContent.get(original));
      }

      // Otherwise treat as file:// URI
      URI uri = new URI(original);
      if (!"file".equalsIgnoreCase(uri.getScheme())) return Optional.empty();
      Path p = Path.of(uri);
      if (Files.isRegularFile(p)) {
        try {
          return Optional.of(Files.readString(p));
        } catch (IOException e) {
          logger.debug("Failed to read content from file {}", p, e);
          return Optional.empty();
        }
      }
    } catch (URISyntaxException e) { logger.debug("Invalid resource URI: {}", resourceUri, e); }
    return Optional.empty();
  }

  /**
   * Indicates whether the entries were restored from a persisted cache during initialization.
   *
   * @return true if restored from cache; false if freshly built
   */
  @Override
  public boolean loadedFromCache() { return loadedFromCache; }

  private Optional<String> resolveOriginalFromAbstract(String abstractUri) {
    if (abstractUri == null || !abstractUri.startsWith("kb://")) return Optional.empty();
    String rest = abstractUri.substring("kb://".length());
    int idx = rest.indexOf('/');
    if (idx <= 0) return Optional.empty();
    String type = rest.substring(0, idx);
    String path = rest.substring(idx + 1);

    try {
      switch (type) {
        case "docs" -> {
          if (foundationRoot == null) return Optional.empty();
          Path p = foundationRoot.resolve("docs").resolve(path);
          return Optional.of(toFileUri(p));
        }
        case "tests" -> {
          if (foundationRoot == null) return Optional.empty();
          Path p = foundationRoot.resolve("tests").resolve(path);
          return Optional.of(toFileUri(p));
        }
        case "feedback" -> {
          if (foundationRoot == null) return Optional.empty();
          Path p = foundationRoot.resolve("feedback").resolve(path);
          return Optional.of(toFileUri(p));
        }
        case "openapi" -> {
          // path: {namespace}/docs/<file>
          int i = path.indexOf('/');
          if (i <= 0) return Optional.empty();
          String namespace = path.substring(0, i);
          String afterNs = path.substring(i + 1);
          String restPath = afterNs.startsWith("docs/") ? afterNs.substring("docs/".length()) : afterNs;
          Path p = Path.of(System.getProperty("java.io.tmpdir"), "external-sdks", namespace, "docs.openapi-generator.markdown").resolve(restPath);
          return Optional.of(toFileUri(p));
        }
        default -> { return Optional.empty(); }
      }
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private void ingestFoundation(Path root) {
    // docs/*.md
    Path docsDir = root.resolve("docs");
    if (Files.isDirectory(docsDir)) {
      try (var stream = Files.list(docsDir)) {
        stream.filter(p -> Files.isRegularFile(p) && hasExtension(p, ".md", ".mdx"))
            .forEach(p -> addFoundationFile(p, "docs", p.getFileName().toString()));
      } catch (IOException e) { logger.debug("Failed to list docs directory {}", docsDir, e); }
    }

    // feedback/* (treat as markdown or text)
    Path feedbackDir = root.resolve("feedback");
    if (Files.isDirectory(feedbackDir)) {
      try (var stream = Files.list(feedbackDir)) {
        stream.filter(Files::isRegularFile)
            .forEach(p -> addFoundationFile(p, "feedback", p.getFileName().toString()));
      } catch (IOException e) { logger.debug("Failed to list feedback directory {}", feedbackDir, e); }
    }

    // tests/* (optional include - they are useful context sometimes)
    Path testsDir = root.resolve("tests");
    if (Files.isDirectory(testsDir)) {
      try (var stream = Files.walk(testsDir)) {
        stream.filter(Files::isRegularFile)
            .forEach(p -> {
              Path rel = testsDir.relativize(p);
              addFoundationFile(p, "tests", rel.toString().replace('\\','/'));
            });
      } catch (IOException e) { logger.debug("Failed to walk tests directory {}", testsDir, e); }
    }

    // specs -> OpenAPI transformation (yaml/json)
    Path specsDir = root.resolve("openapi");
    if (Files.isDirectory(specsDir)) {
      try (var stream = Files.list(specsDir)) {
        AtomicInteger fileIndex = new AtomicInteger(0);
        stream.filter(p -> Files.isRegularFile(p) && hasExtension(p, ".yaml", ".yml", ".json"))
            .forEach((p) -> {
              this.processOpenApiSpec(fileIndex.incrementAndGet(), p);
            });
      } catch (IOException e) { logger.debug("Failed to list specs directory {}", specsDir, e); }
    }
  }

  private void addFoundationFile(Path file, String type, String relativePath) {
    String original = toFileUri(file);
    String abstractUri = "kb://" + type + "/" + relativePath;
    String content = readSafe(file);
    String hint = generateHint(file.getFileName().toString(), content);
    entries.put(abstractUri, new KnowledgeBaseEntry(abstractUri, hint, content));
    abstractToOriginal.put(abstractUri, original);
  }

  private void processOpenApiSpec(Integer fileIndex, Path spec) {
    String outDir = "openapi_" + safeSdkName(spec.getFileName().toString());
    try {
      UploadResult up = tsRuntimeClient.uploadOpenapi(spec, outDir, fileIndex == 1)
          .onErrorResume(e -> {
            logger.error("Failed to upload OpenAPI spec to Typescript runtime", e);
            return Mono.empty();
          }).blockOptional().orElse(null);
      if (up == null || up.sdk() == null || up.sdk().namespace() == null) {
        throw new RuntimeException("Failed to upload OpenAPI spec to Typescript runtime");
      }

      compiledSDKs.put(up.sdk().namespace(), up.sdk().location());

      DocsResponse docs = tsRuntimeClient.fetchDocs(up.sdk().namespace(), false).onErrorResume(e -> Mono.empty()).blockOptional().orElse(null);
      if (docs != null && docs.files() != null && !docs.files().isEmpty()) {
        String baseDisk = docs.diskLocation();
        for (DocFile f : docs.files()) {
          String content = Optional.ofNullable(f.markdown()).orElse("");
          String namespace = docs.namespace();
          String original;
          if (baseDisk != null && !baseDisk.isBlank()) {
            original = toFileUri(Path.of(baseDisk).resolve(f.path()));
          } else {
            // Virtual in-memory resource
            original = "mem://openapi/" + namespace + "/" + f.path();
            inMemoryContent.put(original, content);
          }
          String abstractUri = "kb://openapi/" + namespace + "/docs/" + f.path();
          String hint = generateHint(f.path(), content);
          entries.put(abstractUri, new KnowledgeBaseEntry(abstractUri, hint, content));
          abstractToOriginal.put(abstractUri, original);
        }
      }
    } catch (Exception e) {
      // On any error, at least index the raw spec file under openapi type
      logger.warn("Failed to process OpenAPI spec {}, indexing raw file instead", spec, e);
      addFoundationFile(spec, "openapi", spec.getFileName().toString());
    }
  }

  private String generateHint(String name, String content) {
    // Prefer a short LLM-produced hint, fallback to heuristic first lines
    try {
      String txt;
      if(aiHintGenerationEnabled) {
        String prompt = "Given the following file name and content, produce a single concise sentence (max 30 words) that describes what information this document contains, focusing on how an engineer would use it.\n" +
            "File: " + name + "\n\n" +
            "Content (first 800 chars):\n" + truncate(content, 800);
        InferenceResponse resp = inferenceService.sendRequest(prompt, List.of());
        txt = Optional.ofNullable(resp).map(InferenceResponse::content).orElse("");
      } else {
        txt = content.substring(0, Math.min(content.length(), hintContentLimit)) + "...";
      }
      txt = sanitizeHint(txt, hintContentLimit);
      if (!txt.isBlank()) return txt;
    } catch (Exception ignored) { }
    // Fallback
    String first = Arrays.stream(content.split("\n"))
        .map(String::trim)
        .filter(s -> !s.isBlank())
        .findFirst().orElse(name);
    return (first.length() > hintContentLimit) ? first.substring(0, hintContentLimit) + "…" : first;
  }

  private static String sanitizeHint(String s, int hintSize) {
    if (s == null) return "";
    s = s.replaceAll("^[\"'`]+|[\"'`]+$", ""); // trim quotes
    s = s.replaceAll("\n+", " ").trim();
    if (s.length() > hintSize) s = s.substring(0, hintSize) + "…";
    return s;
  }

  private static String truncate(String s, int max) {
    if (s == null) return "";
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    if (bytes.length <= max) return s;
    return new String(Arrays.copyOf(bytes, max), StandardCharsets.UTF_8);
  }

  private static boolean hasExtension(Path p, String... exts) {
    String n = p.getFileName().toString().toLowerCase(Locale.ROOT);
    for (String e : exts) if (n.endsWith(e)) return true;
    return false;
  }

  private static String toFileUri(Path p) { return p.toAbsolutePath().toUri().toString(); }
  private static String readSafe(Path p) { try { return Files.readString(p); } catch (IOException e) { return ""; } }

  private static String safeSdkName (String name) {
    return stripExt(name).replaceAll("[^a-zA-Z0-9]", "_");
  }

  private static String stripExt(String name) {
    int i = name.lastIndexOf('.') ;
    return (i > 0) ? name.substring(0, i) : name;
  }

  private static String computeSignature(Path root) {
    // Simple signature: concatenate file names + lastModified for known subtrees
    List<Path> toScan = new ArrayList<>();
    toScan.add(root.resolve("docs"));
    toScan.add(root.resolve("openapi"));
    toScan.add(root.resolve("agent.yaml"));
    toScan.add(root.resolve("feedback"));
    StringBuilder sb = new StringBuilder();
    for (Path dir : toScan) {
      if (Files.isDirectory(dir)) {
        try (var walk = Files.walk(dir)) {
          walk.filter(Files::isRegularFile).sorted()
              .forEach(f -> {
                try {
                  sb.append(f.toAbsolutePath()).append('|').append(Files.getLastModifiedTime(f).toMillis()).append('\n');
                } catch (IOException e) { logger.debug("Failed to read lastModifiedTime for {}", f, e); }
              });
        } catch (IOException e) { logger.debug("Failed to walk directory {}", dir, e); }
      } else if (Files.isRegularFile(dir)) {
        try {
          sb.append(dir.toAbsolutePath()).append('|').append(Files.getLastModifiedTime(dir).toMillis()).append('\n');
        } catch (IOException e) { logger.debug("Failed to read lastModifiedTime for {}", dir, e); }
      }
    }
    return Integer.toHexString(sb.toString().hashCode());
  }
}
