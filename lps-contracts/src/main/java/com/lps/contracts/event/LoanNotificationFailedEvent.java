package com.lps.contracts.event;
import java.time.Instant;
import java.util.UUID;
/** Technical notification failure; does not reverse the approved business outcome. */
public record LoanNotificationFailedEvent(UUID eventId, String eventType, Instant occurredAt,
        UUID correlationId, UUID applicationId, UUID commandId, String errorCode,
        boolean retryable) implements ProcessingFailureEvent {}
