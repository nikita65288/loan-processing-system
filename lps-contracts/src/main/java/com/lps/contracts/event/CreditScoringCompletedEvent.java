package com.lps.contracts.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Fact that credit scoring has completed for a loan application.
 */
public record CreditScoringCompletedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        Integer score,
        boolean approved
) {
}
