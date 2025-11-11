package com.gentoro.onemcp.messages;

import java.util.List;

public record Documentation(
    @FieldDoc(
            example = "serviceX",
            description =
                "Name of the services from which the available operations should be listed.",
            required = true)
        List<String> services) {}
