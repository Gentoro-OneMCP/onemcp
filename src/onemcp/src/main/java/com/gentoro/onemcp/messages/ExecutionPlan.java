package com.gentoro.onemcp.messages;

import com.gentoro.onemcp.utility.StringUtility;
import java.util.List;

public record ExecutionPlan(
    @FieldDoc(description = "Collection of steps defining and execution plan.", required = true)
        List<Step> steps) {

  public record Step(
      @FieldDoc(description = "A concise label for this execution step.", required = true)
          String title,
      @FieldDoc(
              description =
                  "Collection of services and their corresponding operation names to be used during the implementation of the step.",
              required = true)
          List<Service> services,
      @FieldDoc(
              example =
                  "Multiline, rich and descriptive description of the snippet to be created and executed.",
              description = "Detailed narrative of what this step accomplishes.",
              multiline = true)
          String description) {
    public record Service(
        @FieldDoc(
                example = "serviceX",
                description =
                    "The name of the service, from the provided list of available services",
                required = true)
            String serviceName,
        @FieldDoc(
                example = "getXYZ",
                description =
                    "List of operations from the given service that should be used to implement the step.",
                required = true)
            List<String> operations) {}
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ExecutionPlan: {\n");
    int index = 0;
    for (Step s : steps) {
      index++;
      sb.append(StringUtility.formatWithIndent("%d. - %s.".formatted(index, s.title()), 4))
          .append("\n");
      sb.append(StringUtility.formatWithIndent(s.description(), 8)).append("\n");
      sb.append(StringUtility.formatWithIndent("- Services", 4)).append("\n");
      for (Step.Service s1 : s.services()) {
        sb.append(StringUtility.formatWithIndent("> %s".formatted(s1.serviceName()), 6))
            .append("\n");
        sb.append(StringUtility.formatWithIndent("- Operations", 8)).append("\n");
        for (String s2 : s1.operations()) {
          sb.append(StringUtility.formatWithIndent(">> %s".formatted(s2), 10)).append("\n");
        }
      }
    }
    sb.append("}\n");
    return sb.toString();
  }
}
