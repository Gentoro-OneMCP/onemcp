package com.gentorox.services.knowledgebase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * File-based implementation of {@link KnowledgeBasePersistence} that stores the knowledge base state as JSON.
 * <p>
 * Behavior:
 * - load(Path): returns Optional.empty() when the path is not a regular file or does not exist.
 * - save(Path, KnowledgeBaseState): ensures the parent directory exists, creates a timestamped backup if the
 *   target file already exists, then writes the new state in pretty-printed JSON format.
 */
@Component
public class FileKnowledgeBasePersistence implements KnowledgeBasePersistence {
  private static final Logger logger = LoggerFactory.getLogger(FileKnowledgeBasePersistence.class);

  private final ObjectMapper objectMapper;

  /**
   * Constructs a new persistence component using a default Jackson {@link ObjectMapper} configured for pretty output.
   */
  public FileKnowledgeBasePersistence() {
    this.objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  }

  /**
   * Loads a {@link KnowledgeBaseState} from a JSON file if it exists and is a regular file.
   *
   * @param file the path to the JSON file; must not be null
   * @return an Optional containing the loaded state, or empty if the file does not exist or is not a regular file
   * @throws IOException if an I/O error occurs while reading or parsing the file
   */
  @Override
  public Optional<KnowledgeBaseState> load(Path file) throws IOException {
    Objects.requireNonNull(file, "file");

    if (Files.isRegularFile(file)) {
      logger.debug("Loading KnowledgeBaseState from {}", file);
      try (var in = Files.newBufferedReader(file)) {
        return Optional.ofNullable(objectMapper.readValue(in, KnowledgeBaseState.class));
      }
    }

    logger.debug("KnowledgeBaseState file not found or not a regular file: {}", file);
    return Optional.empty();
  }

  /**
   * Saves the provided {@link KnowledgeBaseState} to the specified file as pretty-printed JSON.
   * Creates the parent directory if necessary and writes a timestamped backup of the existing file, if present.
   *
   * @param file  the path to write the JSON file to; must not be null
   * @param state the state to persist; must not be null
   * @throws IOException if an I/O error occurs during writing or backup creation
   */
  @Override
  public void save(Path file, KnowledgeBaseState state) throws IOException {
    Objects.requireNonNull(file, "file");
    Objects.requireNonNull(state, "state");

    Path parent = file.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    if (Files.exists(file)) {
      Path backup = file.resolveSibling(file.getFileName() + ".bak." + System.currentTimeMillis());
      logger.info("Existing KnowledgeBaseState detected. Creating backup: {} -> {}", file, backup);
      Files.move(file, backup);
    }

    logger.debug("Saving KnowledgeBaseState to {}", file);
    try (var out = Files.newBufferedWriter(file)) {
      objectMapper.writeValue(out, state);
    }
  }
}
