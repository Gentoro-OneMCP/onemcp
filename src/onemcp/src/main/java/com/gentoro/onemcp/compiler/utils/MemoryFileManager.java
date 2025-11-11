package com.gentoro.onemcp.compiler.utils;

import java.util.HashMap;
import java.util.Map;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;

public class MemoryFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {
  private final Map<String, InMemoryClassFile> compiledClasses = new HashMap<>();

  public MemoryFileManager(StandardJavaFileManager standardManager) {
    super(standardManager);
  }

  @Override
  public JavaFileObject getJavaFileForOutput(
      Location location, String className, JavaFileObject.Kind kind, FileObject sibling) {
    InMemoryClassFile file = new InMemoryClassFile(className, kind);
    compiledClasses.put(className, file);
    return file;
  }

  public Map<String, byte[]> getAllClassBytes() {
    Map<String, byte[]> result = new HashMap<>();
    for (var e : compiledClasses.entrySet()) {
      result.put(e.getKey(), e.getValue().getBytes());
    }
    return result;
  }
}
