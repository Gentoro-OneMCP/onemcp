package com.gentoro.onemcp.utility;

import com.gentoro.onemcp.OneMcp;
import com.gentoro.onemcp.exception.ExceptionUtil;

public class StdoutUtility {
  private static final String green = "\u001B[32m";
  private static final String red = "\u001B[31m";
  private static final String reset = "\u001B[0m";

  public static void printRollingLine(OneMcp oneMcp, String message) {
    if (oneMcp.isInteractiveModeEnabled()) {
      System.out.printf("\r%s", String.join("\\n", message.split("\n")));
    }
  }

  public static void printSuccessLine(OneMcp oneMcp, String message) {
    if (oneMcp.isInteractiveModeEnabled()) {
      System.out.print("\r✅ ");
      for (String line : message.split("\n")) {
        System.out.printf("%s%s%s%n", green, line, reset);
      }
    }
  }

  public static void printNewLine(OneMcp oneMcp, String message) {
    if (oneMcp.isInteractiveModeEnabled()) {
      System.out.printf("\r%s\n", message);
    }
  }

  public static void printError(OneMcp oneMcp, String message, Throwable cause) {
    if (oneMcp.isInteractiveModeEnabled()) {
      System.out.printf("\r❌ %s%s%s%n", red, message, reset);
      if (cause != null) {
        for (String line : ExceptionUtil.formatCompactStackTrace(cause).split("\n")) {
          System.out.printf("  %s%s%s%n", red, line, reset);
        }
      }
    }
  }
}
