package com.lps.sagaorchestrator.saga;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.model.Decision;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable Saga state and explicit permitted business transitions. */
@Entity
@Table(name = "saga_instances", schema = "saga_orchestrator")
public class SagaInstance {
    @Id
    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    @Column(name = "application_id", nullable = false, updatable = false, unique = true)
    private UUID applicationId;

    @Column(name = "customer_id", nullable = false, updatable = false)
    private UUID customerId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(name = "term", nullable = false)
    private Integer term;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "correlation_id")
    private UUID correlationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private SagaStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step", nullable = false, length = 32)
    private SagaStep currentStep;

    @Column(name = "credit_score")
    private Integer creditScore;

    @Column(name = "bureau_score")
    private Integer bureauScore;

    @Column(name = "has_delinquencies")
    private Boolean hasDelinquencies;

    @Enumerated(EnumType.STRING)
    @Column(name = "final_decision", length = 16)
    private Decision finalDecision;

    @Column(name = "decision_reason", length = 1024)
    private String decisionReason;

    @Column(name="disbursement_id") private UUID disbursementId;
    @Column(name="disbursement_status",length=32) private String disbursementStatus;
    @Column(name="notification_status",length=32) private String notificationStatus;
    @Column(name="notification_failure_code",length=64) private String notificationFailureCode;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "failure_reason", length = 64)
    private String failureReason;

    @Column(name = "failed_at")
    private Instant failedAt;

    @Column(name = "last_error_code", length = 64)
    private String lastErrorCode;

    @Column(name = "last_error_at")
    private Instant lastErrorAt;

    @Column(name = "failed_command_id")
    private UUID failedCommandId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    protected SagaInstance() {}

    public static SagaInstance start(ApplicationCreatedEvent event, Instant now) {
        SagaInstance saga = new SagaInstance();
        saga.sagaId = UUID.randomUUID();
        saga.applicationId = event.applicationId();
        saga.customerId = event.customerId();
        saga.amount = event.amount();
        saga.term = event.term();
        saga.currency = event.currency();
        saga.correlationId = event.correlationId() != null ? event.correlationId() : event.applicationId();
        saga.status = SagaStatus.IN_PROGRESS;
        saga.currentStep = SagaStep.SCORING;
        saga.createdAt = now;
        saga.updatedAt = now;
        return saga;
    }

    /** Older rows may lack correlation_id; derive it from their first completion event. */
    public void completeScoring(Integer score, UUID eventCorrelationId, Instant now) {
        requireStep(SagaStep.SCORING);
        this.creditScore = Objects.requireNonNull(score, "Credit score cannot be null");
        if (this.correlationId == null) {
            this.correlationId = eventCorrelationId != null ? eventCorrelationId : applicationId;
        }
        this.currentStep = SagaStep.BUREAU;
        this.updatedAt = now;
    }

    /** Both bureau indicators must be persisted before creating the decision command. */
    public void completeBureau(Integer score, boolean delinquencies, Instant now) {
        requireStep(SagaStep.BUREAU);
        if (creditScore == null) throw new IllegalStateException("Missing persisted credit score");
        this.bureauScore = Objects.requireNonNull(score, "Bureau score cannot be null");
        this.hasDelinquencies = delinquencies;
        this.currentStep = SagaStep.DECISION;
        this.updatedAt = now;
    }

    /** APPROVED and REJECTED are business outcomes, both complete the technical workflow. */
    public void completeDecision(Decision decision, String reason, Instant now) {
        requireStep(SagaStep.DECISION);
        this.finalDecision = Objects.requireNonNull(decision, "Decision cannot be null");
        if (reason != null && reason.length() > 1024) {
            throw new IllegalArgumentException("Decision reason exceeds 1024 characters");
        }
        this.decisionReason = reason;
        this.currentStep = decision == Decision.APPROVED ? SagaStep.DISBURSEMENT : SagaStep.NOTIFICATION;
        if (decision == Decision.APPROVED) this.disbursementStatus = "PENDING";
        else this.notificationStatus = "PENDING";
        this.updatedAt = now;
    }

    /** The completed financial result is persisted before notification is requested. */
    public void completeDisbursement(UUID id, Instant now) {
        requireStep(SagaStep.DISBURSEMENT);
        if (finalDecision != Decision.APPROVED || id == null)
            throw new IllegalStateException("Only APPROVED Saga can record a payout");
        this.disbursementId=id;
        this.disbursementStatus="COMPLETED";
        this.notificationStatus="PENDING";
        this.currentStep=SagaStep.NOTIFICATION;
        this.updatedAt=now;
    }

    /** Real notification delivery is not claimed; this records only stub completion. */
    public void completeNotification(Instant now) {
        requireStep(SagaStep.NOTIFICATION);
        this.notificationStatus="SENT_SIMULATED";
        finish(now);
    }

    /** Notification exhaustion does not reverse a successful simulated payout. */
    public void failNotification(String reason, Instant now) {
        requireStep(SagaStep.NOTIFICATION);
        this.notificationStatus="FAILED";
        this.notificationFailureCode=Objects.requireNonNull(reason);
        finish(now);
    }

    private void finish(Instant now) {
        this.status=SagaStatus.COMPLETED;
        this.currentStep=SagaStep.FINISHED;
        this.completedAt=now;
        this.updatedAt=now;
    }

    /** Unknown financial outcome is not a definitive business failure. */
    public void requireReconciliation(Instant now) {
        requireStep(SagaStep.DISBURSEMENT);
        this.status=SagaStatus.REQUIRES_RECONCILIATION;
        this.disbursementStatus="UNKNOWN";
        this.failureReason="DISBURSEMENT_UNKNOWN";
        this.failedAt=now;
        this.updatedAt=now;
    }

    /** Technical timeout keeps the last waiting step for diagnostics; never sets business REJECTED. */
    public void failStepTimeout(Instant now) {
        if (status != SagaStatus.IN_PROGRESS || currentStep == SagaStep.FINISHED) {
            throw new IllegalStateException("Cannot time out non-active Saga");
        }
        status = SagaStatus.FAILED;
        failureReason = "STEP_TIMEOUT";
        failedAt = now;
        updatedAt = now;
    }

    /** Retains diagnostics for an explicit retryable error without creating another retry mechanism. */
    public void recordRetryableFailure(String errorCode, Instant now) {
        if (status != SagaStatus.IN_PROGRESS || currentStep == SagaStep.FINISHED) {
            throw new IllegalStateException("Only active Saga steps can record retryable errors");
        }
        lastErrorCode = Objects.requireNonNull(errorCode);
        lastErrorAt = now;
        updatedAt = now;
    }

    /** Permanent execution failure is terminal and preserves the business decision as null. */
    public void failStepPermanently(String errorCode, UUID commandId, Instant now) {
        if (status != SagaStatus.IN_PROGRESS || currentStep == SagaStep.FINISHED) {
            throw new IllegalStateException("Cannot fail terminal Saga");
        }
        failureReason = Objects.requireNonNull(errorCode);
        failedCommandId = Objects.requireNonNull(commandId);
        failedAt = now;
        lastErrorCode = errorCode;
        lastErrorAt = now;
        if (currentStep == SagaStep.DISBURSEMENT) disbursementStatus="FAILED";
        status = SagaStatus.FAILED;
        updatedAt = now;
    }

    private void requireStep(SagaStep expected) {
        if (status != SagaStatus.IN_PROGRESS || currentStep != expected) {
            throw new IllegalStateException("Expected IN_PROGRESS/" + expected + " but was " + status + "/" + currentStep);
        }
    }

    public UUID getSagaId() { return sagaId; }
    public UUID getApplicationId() { return applicationId; }
    public UUID getCustomerId() { return customerId; }
    public BigDecimal getAmount() { return amount; }
    public Integer getTerm() { return term; }
    public String getCurrency() { return currency; }
    public UUID getCorrelationId() { return correlationId; }
    public SagaStatus getStatus() { return status; }
    public SagaStep getCurrentStep() { return currentStep; }
    public Integer getCreditScore() { return creditScore; }
    public Integer getBureauScore() { return bureauScore; }
    public Boolean getHasDelinquencies() { return hasDelinquencies; }
    public Decision getFinalDecision() { return finalDecision; }
    public String getDecisionReason() { return decisionReason; }
    public Instant getCompletedAt() { return completedAt; }
    public String getFailureReason() { return failureReason; }
    public Instant getFailedAt() { return failedAt; }
    public String getLastErrorCode() { return lastErrorCode; }
    public Instant getLastErrorAt() { return lastErrorAt; }
    public UUID getFailedCommandId() { return failedCommandId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public Long getVersion() { return version; }
    public UUID getDisbursementId() { return disbursementId; }
    public String getDisbursementStatus() { return disbursementStatus; }
    public String getNotificationStatus() { return notificationStatus; }
    public String getNotificationFailureCode() { return notificationFailureCode; }
}
