package com.lps.contracts;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Gives stateless command handlers one stable logical result ID per command and event type. */
public final class MessageIdentity {
    private MessageIdentity() {}

    public static UUID resultEventId(UUID commandId, String eventType) {
        Objects.requireNonNull(commandId, "commandId is required");
        Objects.requireNonNull(eventType, "eventType is required");
        return UUID.nameUUIDFromBytes((eventType + ":" + commandId)
                .getBytes(StandardCharsets.UTF_8));
    }
}
