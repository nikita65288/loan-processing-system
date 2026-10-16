package com.lps.sagaorchestrator.retry;

import com.lps.sagaorchestrator.cache.SagaCacheInvalidation;
import com.lps.sagaorchestrator.observability.SagaMetrics;
import com.lps.sagaorchestrator.outbox.SagaOutboxEvent;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.saga.SagaRepository;
import com.lps.sagaorchestrator.saga.SagaStatus;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/** Rechecks the deadline under the same PostgreSQL row lock as completion listeners. */
@Service
public class SagaRetryService {
    private static final Logger log = LoggerFactory.getLogger(SagaRetryService.class);
    private final SagaRepository sagas;
    private final SagaStepAttemptRepository attempts;
    private final SagaOutboxRepository outbox;
    private final SagaRetryProperties policy;
    private final Clock clock;
    private final SagaCacheInvalidation cacheInvalidation;
    private final SagaMetrics metrics;

    public SagaRetryService(SagaRepository sagas, SagaStepAttemptRepository attempts,
                            SagaOutboxRepository outbox, SagaRetryProperties policy, Clock clock,
                            SagaCacheInvalidation cacheInvalidation, SagaMetrics metrics) {
        this.sagas = sagas;
        this.attempts = attempts;
        this.outbox = outbox;
        this.policy = policy;
        this.clock = clock;
        this.cacheInvalidation = cacheInvalidation;
        this.metrics = metrics;
    }

    /** One Saga lock protects decision, attempt insertion and Outbox insertion atomically. */
    @Transactional
    public void process(UUID sagaId) {
        var maybeSaga = sagas.findByIdForUpdate(sagaId);
        if (maybeSaga.isEmpty()) return;
        var saga = maybeSaga.get();
        if (saga.getStatus() != SagaStatus.IN_PROGRESS || saga.getCurrentStep() == SagaStep.FINISHED) return;

        var maybeAttempt = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, saga.getCurrentStep());
        if (maybeAttempt.isEmpty()) return; // Older Saga record without attempt metadata: inspect before recovery.
        SagaStepAttempt current = maybeAttempt.get();
        Instant now = clock.instant();
        if (current.getPublishedAt() == null || current.getDeadlineAt().isAfter(now)) return;
        if (current.getAttemptNumber() >= policy.retry().maxAttempts()) {
            if (saga.getCurrentStep()==SagaStep.DISBURSEMENT) saga.requireReconciliation(now);
            else if (saga.getCurrentStep()==SagaStep.NOTIFICATION) saga.failNotification("NOTIFICATION_TIMEOUT",now);
            else saga.failStepTimeout(now);
            cacheInvalidation.afterCommit(saga.getApplicationId());
            metrics.timeout(saga.getCurrentStep());
            if (saga.getStatus() == SagaStatus.FAILED) metrics.failed(saga, "timeout");
            else if (saga.getStatus() == SagaStatus.COMPLETED) metrics.completed(saga);
            else if (saga.getStatus() == SagaStatus.REQUIRES_RECONCILIATION)
                metrics.reconciliationRequired();
            log.warn("Saga failed after step timeout applicationId={} step={} attempts={}",
                    saga.getApplicationId(), saga.getCurrentStep(), current.getAttemptNumber());
            return;
        }
        if (current.getRetryAfterAt().isAfter(now)) return;

        UUID outboxId = UUID.randomUUID();
        SagaStepAttempt next = SagaStepAttempt.retry(current, outboxId, now);
        attempts.save(next);
        outbox.save(new SagaOutboxEvent(outboxId, sagaId, saga.getApplicationId(),
                next.getCommandType(), next.getTopic(), next.getPayload(), now));
        metrics.retry(saga.getCurrentStep());
        log.info("Saga business retry queued applicationId={} step={} attempt={} logicalCommandId={} outboxId={}",
                saga.getApplicationId(), saga.getCurrentStep(), next.getAttemptNumber(),
                next.getCommandId(), outboxId);
    }
}
