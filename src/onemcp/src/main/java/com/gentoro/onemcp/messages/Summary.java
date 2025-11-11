package com.gentoro.onemcp.messages;

public record Summary(
    @FieldDoc(
            description =
                "Concise answer based on the provided values, and answering the original assignment",
            required = true)
        String answer,
    @FieldDoc(description = "The detailed reasoning that originated the answer", required = true)
        String reasoning) {}
