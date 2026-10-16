package com.lps.contracts.event;

import com.lps.contracts.model.Decision;

import java.time.Instant;
import java.util.UUID;

/**
 * Fact that a loan decision has been completed.
 */
public record DecisionCompletedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        Decision decision,
        String reason
) {
}
