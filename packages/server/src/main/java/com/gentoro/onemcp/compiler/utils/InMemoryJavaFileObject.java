package com.gentoro.onemcp.compiler.utils;

import java.net.URI;
import javax.tools.SimpleJavaFileObject;

public class InMemoryJavaFileObject extends SimpleJavaFileObject {
  private final String sourceCode;

  public InMemoryJavaFileObject(String className, String sourceCode) {
    super(
        URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension),
        Kind.SOURCE);
    this.sourceCode = sourceCode;
  }

  @Override
  public CharSequence getCharContent(boolean ignoreEncodingErrors) {
    return sourceCode;
  }
}
