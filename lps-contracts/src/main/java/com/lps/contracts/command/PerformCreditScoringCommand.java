package com.lps.contracts.command;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Request to perform credit scoring for a loan application.
 */
public record PerformCreditScoringCommand(
        UUID commandId,
        String commandType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        UUID customerId,
        BigDecimal amount,
        Integer term,
        String currency
) {
}
