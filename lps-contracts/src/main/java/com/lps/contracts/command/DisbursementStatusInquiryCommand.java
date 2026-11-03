package com.lps.contracts.command;

import java.time.Instant;
import java.util.UUID;

/** Read-only request for the durable outcome of an existing logical payout command. */
public record DisbursementStatusInquiryCommand(UUID inquiryId, String commandType, Instant occurredAt,
        UUID correlationId, UUID applicationId, UUID disbursementCommandId) {}
