package com.lps.contracts.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Fact that the bureau check has completed for a loan application.
 */
public record BureauCheckCompletedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        Integer bureauScore,
        boolean hasDelinquencies,
        boolean approved
) {
}
