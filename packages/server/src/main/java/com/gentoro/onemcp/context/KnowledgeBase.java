package com.gentoro.onemcp.context;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.*;
import com.gentoro.onemcp.openapi.MarkdownGenerator;
import com.gentoro.onemcp.openapi.OpenApiLoader;
import com.gentoro.onemcp.utility.FileUtility;
import com.gentoro.onemcp.utility.JacksonUtility;
import com.gentoro.onemcp.utility.StringUtility;
import io.swagger.v3.oas.models.OpenAPI;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * KnowledgeBase loads and serves Markdown documents as a lightweight knowledge repository.
 *
 * <p>Documents are discovered at startup from the directory configured via configuration key {@code
 * context.storage.dir}. Each file is exposed as a {@link KnowledgeDocument} with a stable URI of
 * the form {@code kb:///relative/path.md}, where the path is relative to the configured root
 * directory.
 *
 * <p>This component provides simple retrieval helpers to list documents by exact URIs and to search
 * by URI prefix.
 */
public class KnowledgeBase {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(KnowledgeBase.class);

  private final List<KnowledgeDocument> documents = new ArrayList<>();
  private final List<Service> services = new ArrayList<>();
  private final OneMcp oneMcp;
  private Path handbookLocation = null;

  /**
   * Create a new knowledge base bound to the provided configuration.
   *
   * @param oneMcp Main entry point for OneMCP application context
   */
  public KnowledgeBase(OneMcp oneMcp) {
    this.oneMcp = oneMcp;
  }

  /**
   * Processes and loads the knowledge base by ingesting Markdown documents and generating resources
   * from OpenAPI definitions.
   *
   * <p>This method performs two main operations: 1. Invokes {@code loadDocuments()} to retrieve and
   * cache all Markdown files located in the configured storage directory as {@link
   * KnowledgeDocument} instances. 2. Executes {@code processOpenApiDefinitions()} to process
   * OpenAPI YAML files found in a subdirectory named "openapi". It converts these files into
   * corresponding Markdown resources, ensuring that the knowledge base is populated with generated
   * content.
   *
   * @throws IOException if file operations fail or resources cannot be read/written during the
   *     process
   * @throws IllegalArgumentException if the storage directory is missing, invalid, or lacks
   *     required resources
   */
  public void ingestHandbook() throws IOException {
    processOpenApiDefinitions();
    loadDocuments();
    validateHandbookStructure();
  }

  private void validateHandbookStructure() {
    if (documents.stream().noneMatch(e -> e.uri().startsWith("kb:///instructions.md"))) {
      throw new ValidationException("Handbook directory must contain instructions.md");
    }
  }

  /**
   * Retrieves an immutable list of services available in the knowledge base.
   *
   * @return an immutable copy of the list of services
   */
  public List<Service> services() {
    return List.copyOf(services);
  }

  private void processOpenApiDefinitions() {
    Path handbookPath = handbookPath();
    try {
      Files.walk(handbookPath.resolve(Path.of("openapi")))
          .forEach(
              file -> {
                if (Files.isRegularFile(file)
                    && file.getFileName().toString().toLowerCase().endsWith(".yaml")) {
                  services.add(initializeService(handbookPath, file));
                }
              });
    } catch (Exception e) {
      throw ExceptionUtil.rethrowIfUnchecked(
          e,
          (ex) ->
              new IoException(
                  "Failed to process OpenAPI definitions under the path: " + handbookPath, ex));
    }

    if (services.isEmpty()) {
      throw new ConfigException(
          "No OpenAPI definitions found in: "
              + handbookPath
              + ". OpenAPI definitions must be located under the path: `"
              + handbookPath
              + "/openapi`, and must be named with the extension `.yaml`.");
    }

    log.trace(
        "Populating knowledge base with services: {}",
        services.stream().map(Service::getSlug).collect(Collectors.joining(", ")));

    for (Service srv : services) {
      srv.setOneMcp(oneMcp);
      try {
        documents.add(
            new KnowledgeDocument(
                "kb:///services/%s/README.md".formatted(srv.getSlug()),
                readHandbookContent(srv.getReadmeUri())));
      } catch (Exception e) {
        throw ExceptionUtil.rethrowIfUnchecked(
            e, (ex) -> new IoException("Failed to read service README: " + srv.getReadmeUri(), ex));
      }

      for (Operation op : srv.getOperations()) {
        try {
          op.setService(srv);
          op.setOneMcp(oneMcp);
          documents.add(
              new KnowledgeDocument(
                  "kb:///services/%s/operations/%s.md".formatted(srv.getSlug(), op.getOperation()),
                  readHandbookContent(op.getDocumentationUri())));
        } catch (Exception e) {
          throw ExceptionUtil.rethrowIfUnchecked(
              e,
              (ex) ->
                  new IoException(
                      "Failed to read operation definition: "
                          + op.getOperation()
                          + ", from service: "
                          + srv.getSlug(),
                      ex));
        }
      }
    }
  }

  public String readHandbookContent(String relativePath) {
    Path handbookPath = handbookPath();
    Path file = handbookPath.resolve(Path.of(relativePath));
    if (!Files.exists(file)) {
      throw new ConfigException("Handbook file does not exist: " + relativePath);
    }
    if (!Files.isRegularFile(file)) {
      throw new ConfigException("Handbook path is not a file: " + relativePath);
    }
    if (!file.getFileName().toString().toLowerCase().endsWith(".md")) {
      throw new ConfigException("Handbook path must end with .md: " + relativePath);
    }
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IoException("Failure while attempting to read a handbook content: " + file, e);
    }
  }

  private Service initializeService(Path root, Path file) {
    log.trace("Initializing service from OpenAPI file: {}", file);
    OpenAPI openApiSpec = OpenApiLoader.load(file.toFile().getAbsolutePath());

    String slug = StringUtility.sanitize(openApiSpec.getInfo().getTitle()).toLowerCase();
    if (slug.isBlank()) {
      slug =
          StringUtility.sanitize(file.getFileName().toString().toLowerCase().replace(".yaml", ""));
    }

    log.trace("Service slug: {}", slug);

    final Path docDir = root.resolve(".services/%s".formatted(slug));
    try {
      if (!Files.exists(docDir)) {
        Files.createDirectories(docDir);
      }
    } catch (Exception e) {
      throw new IoException(
          "There was a problem while attempting to create service directory: "
              + docDir
              + ".\n"
              + "Review if current process has written permission to this location.",
          e);
    }

    AtomicBoolean isServiceInitialized = new AtomicBoolean(false);
    try {
      final Path serviceFile = docDir.resolve("service.yaml");
      long checksum = -1L;
      try {
        checksum = Files.readString(file, StandardCharsets.UTF_8).hashCode();
      } catch (Exception e) {
        throw new IoException(
            "There was a problem while attempting to load service definition: "
                + file
                + ".\n"
                + "It might be related to file permission or corrupted content.",
            e);
      }

      if (Files.exists(serviceFile)) {
        log.trace(
            "Prior compiled version of the service detected at: {}.\nLoading definition and comparing checksum.",
            serviceFile);
        try {
          Service cachedService =
              JacksonUtility.getYamlMapper().readValue(serviceFile.toFile(), Service.class);
          if (cachedService.getChecksum() == checksum) {
            isServiceInitialized.set(true);
            // nothing has changed, skip loading the service definition
            log.trace(
                "Service definition not changed, using already compiled version: {}", serviceFile);
            return cachedService;
          }
        } catch (Exception e) {
          throw new IoException(
              "There was a problem while attempting to load prior service definition: "
                  + serviceFile
                  + ".\n"
                  + "It might be related to file permission or corrupted content.",
              e);
        }
      }

      final Path readmeFile = docDir.resolve("README.md");
      final Service service =
          new Service(
              checksum,
              slug,
              root.relativize(file).toString(),
              root.relativize(docDir).toString(),
              root.relativize(readmeFile).toString(),
              new ArrayList<>());

      String title = openApiSpec.getInfo().getTitle();
      if (title == null || title.isBlank()) {
        title = file.getFileName().toString().replace(".yaml", "");
      }

      try {
        Files.writeString(
            readmeFile,
            oneMcp
                .promptRepository()
                .get("/openapi")
                .newSession()
                .enableOnly(
                    "readme",
                    Map.of(
                        "title",
                        title,
                        "description",
                        StringUtility.formatWithIndent(openApiSpec.getInfo().getDescription(), 2)))
                .renderText());
      } catch (Exception e) {
        throw new IoException(
            "There was a problem while attempting to generate service README: "
                + readmeFile
                + ".\n"
                + "Review if current process has written permission to this location.",
            e);
      }

      List<Operation> operations = new ArrayList<>(service.getOperations());
      for (Map<String, Object> entry : MarkdownGenerator.generate(openApiSpec)) {
        Path operationFile = docDir.resolve("%s.md".formatted(entry.get("operation")));
        try {
          Files.writeString(
              operationFile,
              oneMcp
                  .promptRepository()
                  .get("/openapi")
                  .newSession()
                  .enableOnly("operation", entry)
                  .renderText());
        } catch (Exception e) {
          throw new IoException(
              "There was a problem while attempting to generate operation: "
                  + operationFile
                  + ".\n"
                  + "Review if current process has written permission to this location.",
              e);
        }
        operations.add(
            new Operation(
                root.relativize(operationFile).toString(),
                entry.get("operation").toString(),
                entry.get("method").toString(),
                entry.get("path").toString(),
                entry.get("summary").toString()));
      }
      service.setOperations(operations);
      try {
        JacksonUtility.getYamlMapper().writeValue(serviceFile.toFile(), service);
        isServiceInitialized.set(true);
      } catch (Exception e) {
        throw new IoException(
            "There was a problem while attempting to save service definition: "
                + serviceFile
                + ".\n"
                + "It might be related to file permission or corrupted content.",
            e);
      }
      return service;
    } finally {
      if (!isServiceInitialized.get()) {
        log.warn("Service was not initialized properly, deleting generated files at: {}", docDir);
        FileUtility.deleteDir(docDir, true);
      }
    }
  }

  /**
   * Load all Markdown files from the directory configured by key {@code context.storage.dir}.
   *
   * <p>All files under that directory (recursively) with extension {@code .md} are read as UTF-8,
   * converted to {@link KnowledgeDocument} instances and cached in-memory. Non-readable files are
   * skipped.
   */
  private void loadDocuments() {
    Path handbookPath = handbookPath();
    try {
      try (var paths = Files.walk(handbookPath)) {
        log.trace("Loading documents from handbook: {}", handbookPath);
        paths
            .filter(Files::isRegularFile)
            .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".md"))
            .forEach(file -> addMarkdownFile(handbookPath, file));
      }
    } catch (IOException e) {
      throw new IoException("Failed to read markdown files from handbook", e);
    }
  }

  private void addMarkdownFile(Path root, Path file) {
    String name = file.getFileName().toString();
    if (!name.toLowerCase().endsWith(".md")) {
      throw new ValidationException("Markdown file must end with .md: " + name);
    }
    try {
      String relativePath = "kb:///" + root.relativize(file);
      String content = Files.readString(file, StandardCharsets.UTF_8);
      documents.add(new KnowledgeDocument(relativePath, content));
      log.trace("Registered document to KB: {}", relativePath);
    } catch (IOException e) {
      throw new IoException("Failed to read file: " + file, e);
    }
  }

  public KnowledgeDocument getDocument(String uri) {
    log.trace("Fetching document: {}", uri);
    return documents.stream()
        .filter(e -> e.uri().equals(uri))
        .findFirst()
        .orElseThrow(
            () -> new NotFoundException("Document not found: " + uri));
  }

  /**
   * Retrieve all documents by their exact URIs.
   *
   * @param uris list of canonical document URIs (e.g., {@code kb:///README.md})
   * @return list of documents whose {@link KnowledgeDocument#uri()} is present in the input list
   */
  public List<KnowledgeDocument> findAllByUris(List<String> uris) {
    return documents.stream().filter(e -> uris.contains(e.uri())).collect(Collectors.toList());
  }

  /**
   * Find all documents whose URI starts with the given prefix.
   *
   * <p>If the prefix does not start with {@code kb://}, it will be automatically prepended. This
   * allows using either {@code README} or {@code kb:///README} when searching.
   *
   * @param uriPrefix URI prefix to match (with or without {@code kb://})
   * @return list of matching documents (possibly empty)
   */
  public List<KnowledgeDocument> findByUriPrefix(String uriPrefix) {
    if (!uriPrefix.startsWith("kb://")) {
      uriPrefix = "kb://" + uriPrefix;
    }
    final String finalQuery = uriPrefix;
    return documents.stream()
        .filter(e -> e.uri().startsWith(finalQuery))
        .collect(Collectors.toList());
  }

  public Path handbookPath() {
    if (Objects.nonNull(handbookLocation)) {
      return handbookLocation;
    }

    String location = oneMcp.configuration().getString("handbook.location");
    if (location == null || location.isBlank()) {
      throw new ConfigException("Missing handbook.location config");
    }

    location = location.trim();
    log.trace("Resolving handbook location: {}", location);

    // Classpath-based handbook location
    if (location.startsWith("classpath:")) {
      String base = location.substring("classpath:".length());
      if (base.startsWith("/")) {
        log.warn("Classpath handbook location should not start with a slash: {}", location);
        base = base.substring(1);
      }
      if (base.startsWith("resources/")) {
        log.warn("Classpath handbook location should not start with 'resources/': {}", location);
        base = base.substring("resources/".length());
      }

      if (base.isBlank()) {
        throw new ConfigException("Invalid handbook.location: classpath base path is empty");
      }

      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) cl = KnowledgeBase.class.getClassLoader();
      URL url = cl.getResource(base);
      if (url == null) {
        throw new ConfigException("Classpath handbook directory not found: " + location);
      }

      try {
        log.trace("Resolving classpath handbook location: {}", url);
        URI uri = url.toURI();
        Path sourcePath;
        FileSystem fs = null;
        boolean createdFs = false;
        try {
          sourcePath = Path.of(uri);
        } catch (FileSystemNotFoundException e) {
          // Likely a JAR resource, mount a new filesystem
          fs = FileSystems.newFileSystem(uri, Map.of());
          createdFs = true;
          sourcePath = Path.of(uri);
        } catch (FileSystemAlreadyExistsException e) {
          sourcePath = Path.of(uri);
        }

        if (!Files.isDirectory(sourcePath)) {
          throw new ConfigException("Classpath handbook path is not a directory: " + location);
        }

        // Copy classpath directory to a temporary writable directory so that generated files
        // (e.g., .services) can be written without mutating the classpath.
        Path tempDir = Files.createTempDirectory("handbook-");
        log.info(
            "Handbook is located on a readonly file system, copying classpath handbook directory to temporary directory ({})",
            tempDir);
        FileUtility.copyDirectory(sourcePath, tempDir);

        if (fs != null && createdFs) {
          try {
            fs.close();
          } catch (IOException ignored) {
          }
        }

        handbookLocation = tempDir;
        log.trace("Assigned handbook location: {}", handbookLocation);
        return tempDir;
      } catch (URISyntaxException | IOException ex) {
        throw new KnowledgeBaseException(
            "Failed to resolve classpath handbook directory: " + location, ex);
      }
    }

    // Filesystem-based location (absolute or relative path, or file: URI)
    Path basePath;
    try {
      if (location.startsWith("file:")) {
        basePath = Path.of(URI.create(location));
      } else {
        basePath = Path.of(location);
      }
    } catch (Exception iae) {
      throw new ConfigException("Invalid handbook.location URI/path: " + location, iae);
    }

    if (!Files.exists(basePath)) {
      throw new ConfigException("Handbook directory does not exist: " + basePath);
    }
    if (!Files.isDirectory(basePath)) {
      throw new ConfigException("Handbook path is not a directory: " + basePath);
    }

    handbookLocation = basePath;
    log.trace("Assigned handbook location: {}", handbookLocation);
    return basePath;
  }
}
