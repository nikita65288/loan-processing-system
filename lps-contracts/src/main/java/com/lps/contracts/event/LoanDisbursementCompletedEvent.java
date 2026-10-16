package com.lps.contracts.event;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
/** Immutable confirmation of the one simulated payout for an application. */
public record LoanDisbursementCompletedEvent(UUID eventId, String eventType, Instant occurredAt,
         UUID correlationId, UUID applicationId, UUID commandId, UUID disbursementId,
         BigDecimal amount, String currency) {}
