package com.gentoro.onemcp.indexing.model;

/** Categories of knowledge nodes supported by the v2 indexing model. */
public enum KnowledgeNodeType {
  API_DOCUMENTATION, // api -> documentation
  API_OPERATION_DOCUMENTATION, // api -> operation -> documentation
  API_OPERATION_INPUT, // api -> operation -> input (JSON Schema)
  API_OPERATION_OUTPUT, // api -> operation -> output (JSON Schema)
  API_OPERATION_EXAMPLE, // api -> operation -> example (input+output example)
  DOCS_CHUNK // docs -> chunk
}
