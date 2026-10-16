package com.lps.contracts.command;

import java.time.Instant;
import java.util.UUID;

/**
 * Request to make the final loan decision from scoring and bureau results.
 */
public record MakeDecisionCommand(
        UUID commandId,
        String commandType,
        Instant occurredAt,
        UUID correlationId,
        UUID applicationId,
        Integer creditScore,
        Integer bureauScore,
        boolean hasDelinquencies
) {
}
