package com.gentoro.onemcp.prompt.impl;

import com.gentoro.onemcp.utility.StringUtility;
import io.pebbletemplates.pebble.extension.Function;
import io.pebbletemplates.pebble.template.EvaluationContext;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import java.util.List;
import java.util.Map;

public class IdentFunction implements Function {

  @Override
  public List<String> getArgumentNames() {
    // null means variable arguments are allowed
    return null;
  }

  @Override
  public Object execute(
      Map<String, Object> args, PebbleTemplate self, EvaluationContext context, int lineNumber) {
    return StringUtility.formatWithIndent(
        String.valueOf(args.get("0")), ((Long) args.get("1")).intValue());
  }
}
