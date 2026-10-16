package com.lps.contracts.event;

import java.time.Instant;
import java.util.UUID;

/** Technical execution failure; never represents a business loan rejection. */
public record BureauCheckFailedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        UUID commandId,
        String errorCode,
        boolean retryable
) implements ProcessingFailureEvent {}
