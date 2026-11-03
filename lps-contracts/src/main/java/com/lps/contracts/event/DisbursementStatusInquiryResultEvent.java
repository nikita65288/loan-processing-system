package com.lps.contracts.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Durable snapshot of one read-only status lookup, not an execution result command. */
public record DisbursementStatusInquiryResultEvent(UUID eventId, String eventType, Instant occurredAt,
        UUID correlationId, UUID applicationId, UUID inquiryId, UUID disbursementCommandId,
        DisbursementInquiryStatus status, UUID disbursementId, BigDecimal amount, String currency) {}
