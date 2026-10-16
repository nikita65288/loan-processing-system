package com.lps.sagaorchestrator.observability;

import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaStatus;
import com.lps.sagaorchestrator.saga.SagaStep;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.Duration;
import java.time.Instant;

/** Business counters describe committed state transitions, not Kafka delivery attempts. */
@Component
public class SagaMetrics {
    private static final Logger log = LoggerFactory.getLogger(SagaMetrics.class);
    private final MeterRegistry registry;
    public SagaMetrics(MeterRegistry registry) { this.registry = registry; }

    public void started() {
        afterCommit(() -> registry.counter("lps.saga.started").increment());
    }

    public void completed(SagaInstance saga) {
        if (saga.getStatus() != SagaStatus.COMPLETED || saga.getCompletedAt() == null) return;
        String result = saga.getFinalDecision() == null ? "UNKNOWN" : saga.getFinalDecision().name();
        Instant created = saga.getCreatedAt();
        Instant finished = saga.getCompletedAt();
        afterCommit(() -> {
            registry.counter("lps.saga.completed", "result", result).increment();
            recordDuration(created, finished, "completed");
        });
    }

    public void failed(SagaInstance saga, String category) {
        if (saga.getStatus() != SagaStatus.FAILED) return;
        String step = saga.getCurrentStep().name();
        Instant created = saga.getCreatedAt();
        Instant finished = saga.getFailedAt();
        afterCommit(() -> {
            registry.counter("lps.saga.failed", "step", step, "category", category).increment();
            recordDuration(created, finished, "failed");
        });
    }

    public void timeout(SagaStep step) {
        afterCommit(() -> registry.counter("lps.saga.timeouts", "step", step.name()).increment());
    }

    public void retry(SagaStep step) {
        afterCommit(() -> registry.counter("lps.saga.business.retries", "step", step.name()).increment());
    }

    public void reconciliationRequired() {
        afterCommit(() -> registry.counter("lps.saga.reconciliation.required").increment());
    }

    private void recordDuration(Instant start, Instant end, String result) {
        if (start != null && end != null && !end.isBefore(start)) {
            Timer.builder("lps.saga.duration")
                    .tag("terminal", result)
                    .publishPercentileHistogram()
                    .register(registry)
                    .record(Duration.between(start, end));
        }
    }

    /** Register only during a real transaction; a rollback emits no committed-success metric. */
    private void afterCommit(Runnable metric) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { metric.run(); }
                catch (RuntimeException ex) { log.warn("Saga metric failed after commit", ex); }
            }
        });
    }
}
