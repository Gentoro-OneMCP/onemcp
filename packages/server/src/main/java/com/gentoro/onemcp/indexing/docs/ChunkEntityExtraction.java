package com.gentoro.onemcp.indexing.docs;

import java.util.List;

public final class ChunkEntityExtraction {
  private String chunkId;
  private List<EntityMatch> matches;

  public ChunkEntityExtraction() {}

  public ChunkEntityExtraction(String chunkId, List<EntityMatch> matches) {
    this.chunkId = chunkId;
    this.matches = matches;
  }

  public String getChunkId() {
    return chunkId;
  }

  public List<EntityMatch> getMatches() {
    return matches;
  }

  @Override
  public String toString() {
    return "ChunkEntityExtraction{" + "chunkId='" + chunkId + '\'' + ", matches=" + matches + '}';
  }
}
