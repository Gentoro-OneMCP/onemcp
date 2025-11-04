package com.gentorox.services.knowledgebase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Persistence abstraction for storing and loading the compact knowledge base state.
 */
public interface KnowledgeBasePersistence {
  /**
   * Loads a previously persisted {@link KnowledgeBaseState} from the given file.
   *
   * @param file path to the JSON state file
   * @return optional state if present and readable
   * @throws IOException if an I/O error occurs during reading or parsing
   */
  Optional<KnowledgeBaseState> load(Path file) throws IOException;

  /**
   * Persists the provided {@link KnowledgeBaseState} to the given file.
   *
   * @param file destination file path
   * @param state state to persist
   * @throws IOException if an I/O error occurs while writing
   */
  void save(Path file, KnowledgeBaseState state) throws IOException;
}
