package com.lps.contracts.command;
import com.lps.contracts.model.Decision;
import com.lps.contracts.model.NotificationType;
import java.time.Instant;
import java.util.UUID;
/** Request for a simulated notification (no contact details or external provider). */
public record SendLoanDecisionNotificationCommand(UUID commandId, String commandType, Instant occurredAt,
        UUID correlationId, UUID applicationId, UUID customerId, Decision finalDecision,
        NotificationType notificationType) {}
