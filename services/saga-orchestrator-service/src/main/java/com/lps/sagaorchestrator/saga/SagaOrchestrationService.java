package com.lps.sagaorchestrator.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.contracts.command.SendLoanDecisionNotificationCommand;
import com.lps.contracts.model.Decision;
import com.lps.contracts.model.NotificationType;
import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.event.LoanDisbursementCompletedEvent;
import com.lps.contracts.event.LoanNotificationCompletedEvent;
import com.lps.contracts.event.ProcessingFailureEvent;
import com.lps.sagaorchestrator.cache.SagaCacheInvalidation;
import com.lps.sagaorchestrator.observability.SagaMetrics;
import com.lps.sagaorchestrator.inbox.ProcessedMessageDao;
import com.lps.sagaorchestrator.kafka.SagaKafkaConfiguration;
import com.lps.sagaorchestrator.outbox.SagaOutboxEvent;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.retry.SagaRetryProperties;
import com.lps.sagaorchestrator.retry.SagaStepAttempt;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Orchestrates atomic Inbox registration, durable Saga transitions and Outbox commands. */
@Service
public class SagaOrchestrationService {
    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrationService.class);
    private final SagaRepository sagaRepository;
    private final SagaOutboxRepository outboxRepository;
    private final ProcessedMessageDao inbox;
    private final ObjectMapper mapper;
    private final SagaStepAttemptRepository attempts;
    private final SagaRetryProperties retryProperties;
    private final Clock clock;
    private final SagaCompensationPolicy compensationPolicy;
    private final SagaCacheInvalidation cacheInvalidation;
    private final SagaMetrics sagaMetrics;

    public SagaOrchestrationService(
            SagaRepository sagaRepository,
            SagaOutboxRepository outboxRepository,
            ProcessedMessageDao inbox,
            ObjectMapper mapper,
            SagaStepAttemptRepository attempts,
            SagaRetryProperties retryProperties,
            Clock clock,
            SagaCompensationPolicy compensationPolicy,
            SagaCacheInvalidation cacheInvalidation,
            SagaMetrics sagaMetrics
    ) {
        this.sagaRepository = sagaRepository;
        this.outboxRepository = outboxRepository;
        this.inbox = inbox;
        this.mapper = mapper;
        this.attempts = attempts;
        this.retryProperties = retryProperties;
        this.clock = clock;
        this.compensationPolicy = compensationPolicy;
        this.cacheInvalidation = cacheInvalidation;
        this.sagaMetrics = sagaMetrics;
    }

    /** A transaction-scoped application lock closes the initial no-Saga-row race. */
    @Transactional
    public void initialize(ApplicationCreatedEvent event) {
        inbox.lockApplicationInitialization(event.applicationId());
        if (!inbox.register(event.eventId(), "ApplicationCreated")) return;

        var existing = sagaRepository.findByApplicationIdForUpdate(event.applicationId());
        if (existing.isPresent()) {
            requireSameApplication(existing.get(), event);
            return; // semantic duplicate: record Inbox ID but don't create another command
        }

        Instant now = clock.instant();
        SagaInstance saga = sagaRepository.saveAndFlush(SagaInstance.start(event, now));
        UUID commandId = UUID.randomUUID();
        PerformCreditScoringCommand command = new PerformCreditScoringCommand(
                commandId, "PerformCreditScoring", now, saga.getCorrelationId(),
                saga.getApplicationId(), saga.getCustomerId(), saga.getAmount(),
                saga.getTerm(), saga.getCurrency());
        storeCommand(saga, SagaStep.SCORING, commandId, command.commandType(),
                SagaKafkaConfiguration.SCORING_REQUESTED_TOPIC, command, now);
        cacheInvalidation.afterCommit(saga.getApplicationId());
        sagaMetrics.started();
    }

    /** Repeated deliveries cannot create a second Bureau command. */
    @Transactional
    public void handleCreditScoringCompleted(CreditScoringCompletedEvent event) {
        if (!inbox.register(event.eventId(), "CreditScoringCompleted")) return;
        SagaInstance saga = lockedSaga(event.applicationId());
        if (ignoreLateFailure(saga, event.eventId())) return;
        if (!expectStepOrStale(saga, SagaStep.SCORING)) {
            requireSameResult("Scoring", saga, saga.getCreditScore(), event.score());
            return;
        }
        Instant now = clock.instant();
        saga.completeScoring(event.score(), event.correlationId(), now);
        UUID commandId = UUID.randomUUID();
        PerformBureauCheckCommand command = new PerformBureauCheckCommand(
                commandId, "PerformBureauCheck", now, saga.getCorrelationId(),
                saga.getApplicationId(), saga.getCustomerId());
        storeCommand(saga, SagaStep.BUREAU, commandId, command.commandType(),
                SagaKafkaConfiguration.BUREAU_REQUESTED_TOPIC, command, now);
        cacheInvalidation.afterCommit(saga.getApplicationId());
    }

    /** Only one successful Bureau transition may enqueue a Decision command. */
    @Transactional
    public void handleBureauCheckCompleted(BureauCheckCompletedEvent event) {
        if (!inbox.register(event.eventId(), "BureauCheckCompleted")) return;
        SagaInstance saga = lockedSaga(event.applicationId());
        if (ignoreLateFailure(saga, event.eventId())) return;
        if (!expectStepOrStale(saga, SagaStep.BUREAU)) {
            requireSameResult("Bureau score", saga, saga.getBureauScore(), event.bureauScore());
            requireSameResult("Bureau delinquencies", saga, saga.getHasDelinquencies(), event.hasDelinquencies());
            return;
        }
        Instant now = clock.instant();
        saga.completeBureau(event.bureauScore(), event.hasDelinquencies(), now);
        UUID commandId = UUID.randomUUID();
        MakeDecisionCommand command = new MakeDecisionCommand(
                commandId, "MakeDecision", now, saga.getCorrelationId(),
                saga.getApplicationId(), saga.getCreditScore(), saga.getBureauScore(),
                saga.getHasDelinquencies());
        storeCommand(saga, SagaStep.DECISION, commandId, command.commandType(),
                SagaKafkaConfiguration.DECISION_REQUESTED_TOPIC, command, now);
        cacheInvalidation.afterCommit(saga.getApplicationId());
    }

    /** Terminal decisions are immutable; a conflicting replay remains a processing error. */
    @Transactional
    public void handleDecisionCompleted(DecisionCompletedEvent event) {
        if (!inbox.register(event.eventId(), "DecisionCompleted")) return;
        SagaInstance saga = lockedSaga(event.applicationId());
        if (ignoreLateFailure(saga, event.eventId())) return;
        if (!expectStepOrStale(saga, SagaStep.DECISION)) {
            requireSameResult("Decision", saga, saga.getFinalDecision(), event.decision());
            requireSameResult("Decision reason", saga, saga.getDecisionReason(), event.reason());
            return;
        }
        Instant now=clock.instant();
        saga.completeDecision(event.decision(), event.reason(), now);
        if(event.decision()==Decision.APPROVED) {
            UUID commandId=UUID.randomUUID();
            DisburseLoanCommand command=new DisburseLoanCommand(commandId,"DisburseLoan",now,
                    saga.getCorrelationId(),saga.getApplicationId(),saga.getAmount(),saga.getCurrency());
            storeCommand(saga,SagaStep.DISBURSEMENT,commandId,command.commandType(),
                    SagaKafkaConfiguration.DISBURSEMENT_REQUESTED_TOPIC,command,now);
        } else {
            queueNotification(saga,now);
        }
        cacheInvalidation.afterCommit(saga.getApplicationId());
    }

    /** Only a confirmation for the pending original command may advance an approved Saga. */
    @Transactional
    public void handleDisbursementCompleted(LoanDisbursementCompletedEvent event) {
        if(!inbox.register(event.eventId(),"LoanDisbursementCompleted")) return;
        SagaInstance saga=lockedSaga(event.applicationId());
        if(ignoreLateFailure(saga,event.eventId())) return;
        if(!expectStepOrStale(saga,SagaStep.DISBURSEMENT)) {
            requireSameResult("Disbursement id",saga,saga.getDisbursementId(),event.disbursementId());
            return;
        }
        verifyStepResult(saga,SagaStep.DISBURSEMENT,event.commandId(),event.correlationId());
        if(event.disbursementId()==null || event.amount()==null
                || saga.getAmount().compareTo(event.amount())!=0
                || !saga.getCurrency().equals(event.currency()))
            throw new IllegalStateException("Conflicting payout result for "+saga.getApplicationId());
        Instant now=clock.instant();
        saga.completeDisbursement(event.disbursementId(),now);
        queueNotification(saga,now);
        cacheInvalidation.afterCommit(saga.getApplicationId());
    }

    /** A notification outcome never mutates an already-completed simulated payout. */
    @Transactional
    public void handleNotificationCompleted(LoanNotificationCompletedEvent event) {
        if(!inbox.register(event.eventId(),"LoanNotificationCompleted")) return;
        SagaInstance saga=lockedSaga(event.applicationId());
        if(ignoreLateFailure(saga,event.eventId())) return;
        // A terminal notification failure is not retroactively replaced by a late success.
        if(saga.getStatus()==SagaStatus.COMPLETED && "FAILED".equals(saga.getNotificationStatus())) return;
        if(!expectStepOrStale(saga,SagaStep.NOTIFICATION)) {
            if(!"SENT_SIMULATED".equals(saga.getNotificationStatus()))
                throw new IllegalStateException("Conflicting terminal notification result");
            return;
        }
        verifyStepResult(saga,SagaStep.NOTIFICATION,event.commandId(),event.correlationId());
        NotificationType expected=saga.getFinalDecision()==Decision.APPROVED
                ? NotificationType.LOAN_APPROVED_AND_DISBURSED : NotificationType.LOAN_REJECTED;
        if(event.notificationType()!=expected) throw new IllegalStateException("Unexpected notification kind");
        saga.completeNotification(clock.instant());
        cacheInvalidation.afterCommit(saga.getApplicationId());
        sagaMetrics.completed(saga);
    }

    private void queueNotification(SagaInstance saga,Instant now) {
        UUID commandId=UUID.randomUUID();
        NotificationType type=saga.getFinalDecision()==Decision.APPROVED
                ? NotificationType.LOAN_APPROVED_AND_DISBURSED : NotificationType.LOAN_REJECTED;
        SendLoanDecisionNotificationCommand command=new SendLoanDecisionNotificationCommand(
                commandId,"SendLoanDecisionNotification",now,saga.getCorrelationId(),
                saga.getApplicationId(),saga.getCustomerId(),saga.getFinalDecision(),type);
        storeCommand(saga,SagaStep.NOTIFICATION,commandId,command.commandType(),
                SagaKafkaConfiguration.NOTIFICATION_REQUESTED_TOPIC,command,now);
    }

    /** Compare to latest durable attempt; every retry retains this logical commandId. */
    private void verifyStepResult(SagaInstance saga,SagaStep step,UUID commandId,UUID correlationId) {
        var tracked=attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(saga.getSagaId(),step)
                .orElseThrow(()->new IllegalStateException("Untracked Saga step"));
        if(!tracked.getCommandId().equals(commandId)
                || !Objects.equals(saga.getCorrelationId(),correlationId))
            throw new IllegalStateException("Mismatching result identity for "+saga.getApplicationId());
    }

    /** One transaction protects Inbox, Saga error metadata and the shared retry deadline. */
    @Transactional
    public void handleFailure(ProcessingFailureEvent event, SagaStep expectedStep) {
        if (event == null || event.eventId() == null || event.applicationId() == null
                || event.commandId() == null || event.occurredAt() == null
                || event.errorCode() == null || !event.errorCode().matches("[A-Z][A-Z0-9_]{0,63}")) {
            throw new IllegalArgumentException("Invalid technical failure contract");
        }
        String expectedType = switch (expectedStep) {
            case SCORING -> "CreditScoringFailed";
            case BUREAU -> "BureauCheckFailed";
            case DECISION -> "DecisionFailed";
            case DISBURSEMENT -> "LoanDisbursementFailed";
            case NOTIFICATION -> "LoanNotificationFailed";
            case FINISHED -> throw new IllegalArgumentException("FINISHED has no failure event");
        };
        if (!expectedType.equals(event.eventType())) {
            throw new IllegalArgumentException("Failure event type does not match Kafka topic/step");
        }
        if (!inbox.register(event.eventId(), expectedType)) return;
        SagaInstance saga = lockedSaga(event.applicationId());
        if (saga.getStatus() != SagaStatus.IN_PROGRESS || isStale(expectedStep, saga.getCurrentStep())) {
            log.warn("Late/stale technical failure ignored applicationId={} eventId={} step={} current={}/{}",
                    event.applicationId(), event.eventId(), expectedStep, saga.getStatus(), saga.getCurrentStep());
            return;
        }
        if (saga.getCurrentStep() != expectedStep) {
            throw new IllegalStateException("Failure for future step " + expectedStep + " of " + saga.getApplicationId());
        }
        if (event.correlationId() != null && saga.getCorrelationId() != null
                && !saga.getCorrelationId().equals(event.correlationId())) {
            throw new IllegalStateException("Failure correlation mismatch applicationId=" + saga.getApplicationId());
        }
        // Retries preserve commandId; it identifies the logical operation, not an attempt.
        SagaStepAttempt latest = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                        saga.getSagaId(), expectedStep)
                .orElseThrow(() -> new IllegalStateException("Untracked step; manual reconciliation required"));
        if (!latest.getCommandId().equals(event.commandId())) {
            throw new IllegalStateException("Failure references unknown logical command applicationId="
                    + saga.getApplicationId());
        }
        Instant now = clock.instant();
        if (event.retryable()) {
            // No early retry: existing published deadline + backoff + maxAttempts remain authoritative.
            saga.recordRetryableFailure(event.errorCode(), now);
            log.warn("Retryable step error recorded applicationId={} step={} code={} attempt={}",
                    event.applicationId(), expectedStep, event.errorCode(), latest.getAttemptNumber());
        } else {
            if (compensationPolicy.requiresCompensation(saga)) {
                throw new IllegalStateException("Compensatable side effect requires durable compensation flow");
            }
            if(expectedStep==SagaStep.NOTIFICATION) saga.failNotification(event.errorCode(),now);
            else saga.failStepPermanently(event.errorCode(), event.commandId(), now);
            log.warn("Saga permanently failed applicationId={} step={} code={} compensation=NOT_REQUIRED",
                    event.applicationId(), expectedStep, event.errorCode());
        }
        cacheInvalidation.afterCommit(saga.getApplicationId());
        if (!event.retryable()) {
            if (saga.getStatus() == SagaStatus.FAILED) sagaMetrics.failed(saga, "permanent");
            else if (saga.getStatus() == SagaStatus.COMPLETED) sagaMetrics.completed(saga);
        }
    }

    /** FAILED is terminal; persist late event in Inbox and acknowledge without replaying workflow. */
    private boolean ignoreLateFailure(SagaInstance saga, UUID eventId) {
        if (saga.getStatus() == SagaStatus.IN_PROGRESS || saga.getStatus() == SagaStatus.COMPLETED) return false;
        log.warn("Late completion ignored after Saga FAILED applicationId={} eventId={} step={}",
                saga.getApplicationId(), eventId, saga.getCurrentStep());
        return true;
    }

    private SagaInstance lockedSaga(UUID applicationId) {
        return sagaRepository.findByApplicationIdForUpdate(applicationId)
                .orElseThrow(() -> new IllegalStateException("Completion event references unknown Saga: " + applicationId));
    }

    /** Past steps may be replayed, but future/unexpected steps must roll back and retry. */
    private boolean expectStepOrStale(SagaInstance saga, SagaStep expected) {
        if (saga.getStatus() == SagaStatus.IN_PROGRESS && saga.getCurrentStep() == expected) return true;
        if ((saga.getStatus() == SagaStatus.COMPLETED && saga.getCurrentStep() == SagaStep.FINISHED)
                || (saga.getStatus() == SagaStatus.IN_PROGRESS && isStale(expected, saga.getCurrentStep()))) {
            return false;
        }
        throw new IllegalStateException("Unexpected completion for applicationId=" + saga.getApplicationId()
                + ": expected " + expected + ", current=" + saga.getStatus() + "/" + saga.getCurrentStep());
    }

    /** Explicit chronology avoids coupling correctness to enum ordinal values. */
    private boolean isStale(SagaStep completed, SagaStep current) {
        return switch (completed) {
            case SCORING -> current==SagaStep.BUREAU || current==SagaStep.DECISION
                    || current==SagaStep.DISBURSEMENT || current==SagaStep.NOTIFICATION;
            case BUREAU -> current==SagaStep.DECISION || current==SagaStep.DISBURSEMENT
                    || current==SagaStep.NOTIFICATION;
            case DECISION -> current==SagaStep.DISBURSEMENT || current==SagaStep.NOTIFICATION;
            case DISBURSEMENT -> current==SagaStep.NOTIFICATION;
            case NOTIFICATION,FINISHED -> false;
        };
    }

    private void requireSameResult(String field, SagaInstance saga, Object saved, Object incoming) {
        if (!Objects.equals(saved, incoming)) {
            throw new IllegalStateException("Conflicting " + field + " result for applicationId="
                    + saga.getApplicationId() + ": saved=" + saved + ", incoming=" + incoming);
        }
    }

    /** An existing application must match all persisted creation attributes. */
    private void requireSameApplication(SagaInstance saga, ApplicationCreatedEvent event) {
        boolean same = saga.getCustomerId().equals(event.customerId())
                && event.amount() != null && saga.getAmount().compareTo(event.amount()) == 0
                && Objects.equals(saga.getTerm(), event.term())
                && Objects.equals(saga.getCurrency(), event.currency())
                // Older rows may not have persisted correlation_id.
                && (saga.getCorrelationId() == null || Objects.equals(saga.getCorrelationId(),
                        event.correlationId() == null ? event.applicationId() : event.correlationId()));
        if (!same) {
            throw new IllegalStateException("Conflicting ApplicationCreated for applicationId=" + event.applicationId());
        }
    }

    /** Create a command and the first durable step attempt in one transaction. */
    private void storeCommand(SagaInstance saga, SagaStep step, UUID commandId, String type,
                              String topic, Object command, Instant now) {
        try {
            String payload = mapper.writeValueAsString(command);
            outboxRepository.save(new SagaOutboxEvent(commandId, saga.getSagaId(),
                    saga.getApplicationId(), type, topic, payload, now));
            attempts.save(SagaStepAttempt.initial(saga, step, commandId, type, topic,
                    payload, now, retryProperties));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize Saga command " + type, exception);
        }
    }
}
