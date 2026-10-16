package com.lps.contracts.event;

import java.time.Instant;
import java.util.UUID;

/** Shared read-only view for explicit, step-specific technical failure contracts. */
public interface ProcessingFailureEvent {
    UUID eventId();
    String eventType();
    Instant occurredAt();
    UUID correlationId();
    UUID applicationId();
    UUID commandId();
    String errorCode();
    boolean retryable();
}
