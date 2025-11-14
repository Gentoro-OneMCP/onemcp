package com.gentoro.onemcp.prompt.impl;

import com.gentoro.onemcp.exception.ValidationException;
import io.pebbletemplates.pebble.extension.Function;
import io.pebbletemplates.pebble.template.EvaluationContext;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import java.lang.reflect.*;
import java.util.*;

public class ReflectiveCallFunction implements Function {

  @Override
  public List<String> getArgumentNames() {
    // null means variable arguments are allowed
    return null;
  }

  @Override
  public Object execute(
      Map<String, Object> args, PebbleTemplate self, EvaluationContext context, int lineNumber) {
    // Pebble passes arguments in order under keys "0", "1", "2", ...
    Object target = args.get("0");
    String methodName = (String) args.get("1");

    if (target == null || methodName == null) {
      throw new ValidationException("Usage: call(object, methodName, [args...])");
    }

    Class[] types = new Class[args.size() - 2];
    Object[] params = new Object[args.size() - 2];
    for (int i = 2; i < args.size(); i++) {
      params[i - 2] = args.get(String.valueOf(i));
      types[i - 2] = params[i - 2].getClass();
    }
    try {
      // Try to find a matching method by argument count and type
      Method method = target.getClass().getMethod(methodName, types);
      if (method == null) {
        throw new NoSuchMethodException(
            "No method " + methodName + " found on " + target.getClass());
      }

      method.setAccessible(true);
      return method.invoke(target, params);
    } catch (Exception e) {
      throw new com.gentoro.onemcp.exception.PromptException(
          "Error invoking " + methodName + " on " + target.getClass(), e);
    }
  }
}
