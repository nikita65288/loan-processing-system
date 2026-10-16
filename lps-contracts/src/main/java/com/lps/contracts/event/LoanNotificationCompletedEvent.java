package com.lps.contracts.event;
import com.lps.contracts.model.NotificationType;
import java.time.Instant;
import java.util.UUID;
/** SENT_SIMULATED means a durable stub effect, never a real customer delivery. */
public record LoanNotificationCompletedEvent(UUID eventId, String eventType, Instant occurredAt,
        UUID correlationId, UUID applicationId, UUID commandId, NotificationType notificationType) {}
