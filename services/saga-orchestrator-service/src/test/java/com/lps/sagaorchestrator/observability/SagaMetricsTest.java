package com.lps.sagaorchestrator.observability;

import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** No counter can represent a committed effect until afterCommit actually runs. */
class SagaMetricsTest {
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final SagaMetrics metrics = new SagaMetrics(registry);

    @BeforeEach void before() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
    }
    @AfterEach void after() {
        if (TransactionSynchronizationManager.isSynchronizationActive())
            TransactionSynchronizationManager.clearSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(false);
        registry.close();
    }

    @Test void startedOnlyAfterCommit() {
        metrics.started();
        assertNull(registry.find("lps.saga.started").counter());
        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        assertEquals(1.0, registry.counter("lps.saga.started").count());
    }

    @Test void rollbackDoesNotCount() {
        metrics.started();
        TransactionSynchronizationManager.getSynchronizations().forEach(s ->
                s.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
        assertNull(registry.find("lps.saga.started").counter());
    }

    @Test void rejectedDecisionCompletesButDoesNotFail() {
        SagaInstance saga = mock(SagaInstance.class);
        when(saga.getStatus()).thenReturn(SagaStatus.COMPLETED);
        when(saga.getFinalDecision()).thenReturn(Decision.REJECTED);
        when(saga.getCreatedAt()).thenReturn(Instant.parse("2026-01-01T00:00:00Z"));
        when(saga.getCompletedAt()).thenReturn(Instant.parse("2026-01-01T00:01:30Z"));
        metrics.completed(saga);
        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        assertEquals(1, registry.get("lps.saga.completed").tag("result", "REJECTED").counter().count());
        assertEquals(90, registry.get("lps.saga.duration").tag("terminal", "completed").timer().totalTime(java.util.concurrent.TimeUnit.SECONDS));
        assertNull(registry.find("lps.saga.failed").counter());
    }
}
