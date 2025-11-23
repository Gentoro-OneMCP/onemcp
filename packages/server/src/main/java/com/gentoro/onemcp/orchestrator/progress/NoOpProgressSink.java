package com.gentoro.onemcp.orchestrator.progress;

import java.util.Map;

/** No-op implementation used when progress reporting is disabled or not available. */
public class NoOpProgressSink implements ProgressSink {
  @Override
  public void beginStage(String id, String label, long totalWork) {}

  @Override
  public void step(String id, long completed, String message, Map<String, Object> attrs) {}

  @Override
  public void endStageOk(String id, Map<String, Object> attrs) {}

  @Override
  public void endStageError(String id, String errorSummary, Map<String, Object> attrs) {}
}
