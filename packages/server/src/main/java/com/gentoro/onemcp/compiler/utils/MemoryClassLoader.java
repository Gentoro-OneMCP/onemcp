package com.gentoro.onemcp.compiler.utils;

public class MemoryClassLoader extends ClassLoader {
  private final MemoryFileManager fileManager;

  public MemoryClassLoader(MemoryFileManager fileManager) {
    this.fileManager = fileManager;
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    byte[] bytes = fileManager.getAllClassBytes().get(name);
    if (bytes == null) {
      return super.findClass(name);
    }
    return defineClass(name, bytes, 0, bytes.length);
  }
}
