package com.gentorox.core.api;

import com.gentorox.core.model.InferenceRequest;
import com.gentorox.core.model.InferenceResponse;
import java.util.List;

public interface ModelProvider {
  String id();
  InferenceResponse infer(InferenceRequest req, Object... toolInstances);
}
