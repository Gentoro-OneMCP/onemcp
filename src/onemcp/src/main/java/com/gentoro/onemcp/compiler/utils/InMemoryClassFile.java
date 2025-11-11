package com.gentoro.onemcp.compiler.utils;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import javax.tools.SimpleJavaFileObject;

public class InMemoryClassFile extends SimpleJavaFileObject {
  private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

  InMemoryClassFile(String className, Kind kind) {
    super(URI.create("mem:///" + className.replace('.', '/') + kind.extension), kind);
  }

  @Override
  public OutputStream openOutputStream() {
    return outputStream;
  }

  byte[] getBytes() {
    return outputStream.toByteArray();
  }
}
