package com.gentoro.onemcp.indexing.docs;

public final class Chunk {
  private final String id;
  private final String fileName;
  private final String sectionPath; // e.g. "Category > Product"
  private final String content;

  public Chunk(String id, String fileName, String sectionPath, String content) {
    this.id = id;
    this.fileName = fileName;
    this.sectionPath = sectionPath;
    this.content = content;
  }

  public String id() {
    return id;
  }

  public String fileName() {
    return fileName;
  }

  public String sectionPath() {
    return sectionPath;
  }

  public String content() {
    return content;
  }

  @Override
  public String toString() {
    return "Chunk{"
        + "id='"
        + id
        + '\''
        + ", fileName='"
        + fileName
        + '\''
        + ", sectionPath='"
        + sectionPath
        + '\''
        + ", contentLength="
        + (content == null ? 0 : content.length())
        + '}';
  }
}
