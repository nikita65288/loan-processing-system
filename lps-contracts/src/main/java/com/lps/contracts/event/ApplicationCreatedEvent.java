package com.lps.contracts.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Fact that a loan application was created and committed by the Loan Application Service.
 */
public record ApplicationCreatedEvent(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        UUID customerId,
        BigDecimal amount,
        Integer term,
        String currency
) {
}
