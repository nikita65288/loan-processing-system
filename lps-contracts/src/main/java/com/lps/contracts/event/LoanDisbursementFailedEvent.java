package com.lps.contracts.event;
import java.time.Instant;
import java.util.UUID;
/** Explicit failure known before committing a simulated payout. */
public record LoanDisbursementFailedEvent(UUID eventId, String eventType, Instant occurredAt,
        UUID correlationId, UUID applicationId, UUID commandId, String errorCode,
        boolean retryable) implements ProcessingFailureEvent {}
