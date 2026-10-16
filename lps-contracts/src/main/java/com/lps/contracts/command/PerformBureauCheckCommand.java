package com.lps.contracts.command;

import java.time.Instant;
import java.util.UUID;

/**
 * Request to perform a bureau check for a loan application.
 */
public record PerformBureauCheckCommand(
        UUID commandId,
        String commandType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        UUID customerId
) {
}
