package com.lps.sagaorchestrator.api;

import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaStatus;
import com.lps.sagaorchestrator.saga.SagaStep;

import java.time.Instant;
import java.util.UUID;

/** Minimal, non-sensitive status snapshot shared by HTTP responses and Redis. */
public record SagaStatusResponse(
        UUID sagaId,
        UUID applicationId,
        SagaStatus status,
        SagaStep currentStep,
        Decision finalDecision,
        String disbursementStatus,
        String notificationStatus,
        String failureCode,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt
) {
    public static SagaStatusResponse from(SagaInstance saga) {
        return new SagaStatusResponse(saga.getSagaId(), saga.getApplicationId(),
                saga.getStatus(), saga.getCurrentStep(), saga.getFinalDecision(),
                saga.getDisbursementStatus(), saga.getNotificationStatus(),
                saga.getFailureReason(), saga.getCreatedAt(), saga.getUpdatedAt(),
                saga.getCompletedAt());
    }
}
