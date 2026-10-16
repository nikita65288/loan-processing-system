package com.lps.sagaorchestrator.retry;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.cache.SagaCacheInvalidation;
import com.lps.sagaorchestrator.outbox.SagaOutboxEvent;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.saga.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises retry decisions without timers, broker delays or sleeping. */
class SagaRetryServiceTest {
    private static final Instant START = Instant.parse("2026-10-12T09:00:00Z");
    private static final SagaRetryProperties POLICY = new SagaRetryProperties(
            new SagaRetryProperties.Timeout(true, Duration.ofSeconds(10), Duration.ofSeconds(10), Duration.ofSeconds(10)),
            new SagaRetryProperties.Retry(3, Duration.ofSeconds(5)),
            new SagaRetryProperties.Scheduler(50));

    private SagaRepository sagas;
    private SagaOutboxRepository outbox;
    private SagaStepAttemptRepository attempts;
    private SagaInstance saga;
    private SagaStepAttempt first;

    @BeforeEach
    void setup() {
        sagas = mock(SagaRepository.class);
        outbox = mock(SagaOutboxRepository.class);
        attempts = mock(SagaStepAttemptRepository.class);
        var event = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", START,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("3000"), 12, "EUR");
        saga = SagaInstance.start(event, START);
        first = SagaStepAttempt.initial(saga, SagaStep.SCORING, UUID.randomUUID(),
                "PerformCreditScoring", "loan.scoring.requested", "fixed-json-payload", START, POLICY);
        when(sagas.findByIdForUpdate(saga.getSagaId())).thenReturn(Optional.of(saga));
        when(attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(saga.getSagaId(), SagaStep.SCORING))
                .thenReturn(Optional.of(first));
    }

    private void tick(Instant now) {
        new SagaRetryService(sagas, attempts, outbox, POLICY, Clock.fixed(now, ZoneOffset.UTC), mock(SagaCacheInvalidation.class),
                mock(com.lps.sagaorchestrator.observability.SagaMetrics.class))
                .process(saga.getSagaId());
    }

    @Test
    void pendingPublicationCannotTriggerBusinessRetry() {
        tick(START.plusSeconds(100));
        verifyNoInteractions(outbox);
        verify(attempts, never()).save(any());
    }

    @Test
    void priorToDeadlineNoRetry() {
        first.markPublished(START);
        tick(START.plusSeconds(9));
        verifyNoInteractions(outbox);
    }

    @Test
    void timeoutWaitsForBackoff() {
        first.markPublished(START);
        tick(START.plusSeconds(11));
        verifyNoInteractions(outbox);
    }

    @Test
    void timeoutCreatesOneRetryWithNewOutboxIdAndStableBusinessPayload() {
        first.markPublished(START);
        tick(START.plusSeconds(15));
        var attemptCaptor = ArgumentCaptor.forClass(SagaStepAttempt.class);
        var outboxCaptor = ArgumentCaptor.forClass(SagaOutboxEvent.class);
        verify(attempts).save(attemptCaptor.capture());
        verify(outbox).save(outboxCaptor.capture());
        var second = attemptCaptor.getValue();
        var queued = outboxCaptor.getValue();
        assertEquals(2, second.getAttemptNumber());
        assertEquals(first.getCommandId(), second.getCommandId());
        assertEquals(queued.getId(), second.getOutboxEventId());
        assertNotEquals(first.getOutboxEventId(), queued.getId());
        assertEquals(first.getPayload(), queued.getPayload());
        assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
    }

    @Test
    void exhaustedAttemptsFailTechnicallyOnlyAfterFinalDeadline() {
        first.markPublished(START);
        var second = SagaStepAttempt.retry(first, UUID.randomUUID(), START.plusSeconds(15));
        second.markPublished(START.plusSeconds(16));
        var third = SagaStepAttempt.retry(second, UUID.randomUUID(), START.plusSeconds(31));
        third.markPublished(START.plusSeconds(32));
        when(attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(saga.getSagaId(), SagaStep.SCORING))
                .thenReturn(Optional.of(third));
        tick(START.plusSeconds(41));
        assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
        tick(START.plusSeconds(42));
        assertEquals(SagaStatus.FAILED, saga.getStatus());
        assertEquals("STEP_TIMEOUT", saga.getFailureReason());
        assertEquals(START.plusSeconds(42), saga.getFailedAt());
        assertEquals(SagaStep.SCORING, saga.getCurrentStep());
        assertNull(saga.getFinalDecision());
        verifyNoInteractions(outbox);
    }

    @Test
    void failedSagaIsNotRetriedAgain() {
        saga.failStepTimeout(START.plusSeconds(10));
        first.markPublished(START);
        tick(START.plusSeconds(100));
        verify(attempts, never()).save(any());
    }

    @Test
    void completedSagaIsNotRetried() {
        saga.completeScoring(750, saga.getCorrelationId(), START.plusSeconds(1));
        saga.completeBureau(720, false, START.plusSeconds(2));
        saga.completeDecision(com.lps.contracts.model.Decision.APPROVED, "PASS", START.plusSeconds(3));
        saga.completeDisbursement(UUID.randomUUID(),START.plusSeconds(4));
        saga.completeNotification(START.plusSeconds(5));
        tick(START.plusSeconds(100));
        verify(attempts, never()).save(any());
    }
}
