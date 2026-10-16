package com.lps.sagaorchestrator.retry;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.sagaorchestrator.kafka.SagaCommandProducer;
import com.lps.sagaorchestrator.outbox.SagaOutboxPublisher;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.saga.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real PostgreSQL checks atomicity, row locking, deadlines and failure transitions. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false",
        "lps.saga.timeout.scoring=2s",
        "lps.saga.timeout.bureau=2s",
        "lps.saga.timeout.decision=2s",
        "lps.saga.retry.backoff=1s",
        "lps.saga.retry.max-attempts=3"
})
class SagaRetryIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-10-12T09:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired SagaOrchestrationService orchestrator;
    @Autowired SagaRetryService retry;
    @Autowired SagaTimeoutScheduler scheduler;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired SagaOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean SagaCommandProducer producer;
    @MockitoBean Clock clock;
    @MockitoSpyBean SagaOutboxRepository outboxSpy;

    @BeforeEach
    void resetData() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll(); // Attempts are FK ON DELETE CASCADE.
        setClock(NOW);
        when(producer.publish(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private void setClock(Instant instant) {
        when(clock.instant()).thenReturn(instant);
    }

    @Test
    void timeoutCannotStartUntilKafkaAcknowledgesPublication() {
        var event = start();
        var original = latest(event);
        assertNull(original.getPublishedAt());
        setClock(NOW.plusSeconds(100));
        scheduler.checkTimeouts();
        assertEquals(1, attempts.count());
        assertEquals(1, outbox.count());
        publisher.publishPending();
        var confirmed = latest(event);
        assertEquals(NOW.plusSeconds(100), confirmed.getPublishedAt());
        assertEquals(NOW.plusSeconds(102), confirmed.getDeadlineAt());
        scheduler.checkTimeouts();
        assertEquals(1, attempts.count());
    }

    @Test
    void scoringRetriesRetainLogicalCommandIdAndBusinessPayload() {
        var event = start();
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        scheduler.checkTimeouts();
        assertEquals(2, attempts.count());
        var first = attempts.findAll().stream().filter(a -> a.getAttemptNumber() == 1).findFirst().orElseThrow();
        var second = latest(event);
        assertEquals(first.getCommandId(), second.getCommandId());
        assertEquals(first.getPayload(), second.getPayload());
        assertNotEquals(first.getOutboxEventId(), second.getOutboxEventId());
        assertNull(second.getDeadlineAt());
        scheduler.checkTimeouts();
        assertEquals(2, attempts.count());
        publisher.publishPending();
        assertEquals(NOW.plusSeconds(3), latest(event).getPublishedAt());
    }

    @Test
    void bureauStepUsesItsOwnTimeoutAfterSuccessfulScoring() {
        var event = start();
        orchestrator.handleCreditScoringCompleted(scoring(event));
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        scheduler.checkTimeouts();
        assertEquals(SagaStep.BUREAU, saga(event).getCurrentStep());
        var latestBureau = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                saga(event).getSagaId(), SagaStep.BUREAU).orElseThrow();
        assertEquals(2, latestBureau.getAttemptNumber());
        assertEquals("PerformBureauCheck", latestBureau.getCommandType());
    }

    @Test
    void decisionStepUsesItsOwnTimeoutAfterBureau() {
        var event = start();
        orchestrator.handleCreditScoringCompleted(scoring(event));
        orchestrator.handleBureauCheckCompleted(new BureauCheckCompletedEvent(
                UUID.randomUUID(), "BureauCheckCompleted", NOW,
                event.correlationId(), event.applicationId(), 720, false, true));
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        scheduler.checkTimeouts();
        assertEquals(SagaStep.DECISION, saga(event).getCurrentStep());
        var latestDecision = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                saga(event).getSagaId(), SagaStep.DECISION).orElseThrow();
        assertEquals(2, latestDecision.getAttemptNumber());
        assertEquals("MakeDecision", latestDecision.getCommandType());
    }

    @Test
    void failedOutboxPublicationDoesNotStartBusinessTimeout() {
        var event = start();
        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events SET status='FAILED', last_error='Kafka down'
                WHERE application_id=?
                """, event.applicationId());
        setClock(NOW.plusSeconds(300));
        scheduler.checkTimeouts();
        assertEquals(1, attempts.count());
        assertNull(latest(event).getPublishedAt());
        assertEquals(SagaStatus.IN_PROGRESS, saga(event).getStatus());
    }

    @Test
    void exhaustedAttemptsFailWithoutRejectionAndLateEventCannotReopen() {
        var event = start();
        publisher.publishPending();
        for (int i = 1; i <= 2; i++) {
            setClock(NOW.plusSeconds(i * 3L));
            scheduler.checkTimeouts();
            publisher.publishPending();
        }
        assertEquals(3, attempts.count());
        setClock(NOW.plusSeconds(8));
        scheduler.checkTimeouts();
        var saga = saga(event);
        assertEquals(SagaStatus.FAILED, saga.getStatus());
        assertEquals(SagaStep.SCORING, saga.getCurrentStep());
        assertEquals("STEP_TIMEOUT", saga.getFailureReason());
        assertNotNull(saga.getFailedAt());
        assertNull(saga.getFinalDecision());
        long outboxCount = outbox.count();
        orchestrator.handleCreditScoringCompleted(scoring(event));
        assertEquals(SagaStatus.FAILED, saga(event).getStatus());
        assertEquals(outboxCount, outbox.count());
        scheduler.checkTimeouts();
        assertEquals(3, attempts.count());
    }

    @Test
    void lateResultFromFirstAttemptAdvancesCurrentStepOnlyOnce() {
        var event = start();
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        scheduler.checkTimeouts();
        var completion = scoring(event);
        orchestrator.handleCreditScoringCompleted(completion);
        orchestrator.handleCreditScoringCompleted(completion);
        assertEquals(SagaStep.BUREAU, saga(event).getCurrentStep());
        long bureauCommands = outbox.findAll().stream()
                .filter(o -> o.getCommandType().equals("PerformBureauCheck")).count();
        assertEquals(1, bureauCommands);
        setClock(NOW.plusSeconds(50));
        scheduler.checkTimeouts();
        assertEquals(3, attempts.count()); // Scoring 1 + Scoring 2 + Bureau 1, no Scoring 3.
    }

    @Test
    void twoConcurrentSchedulersCreateOneAttempt() throws Exception {
        var event = start();
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        runTwo(() -> retry.process(saga(event).getSagaId()),
                () -> retry.process(saga(event).getSagaId()));
        assertEquals(2, attempts.count());
        assertEquals(2, outbox.count());
    }

    @Test
    void completionRacingTimeoutCannotDuplicateNextStep() throws Exception {
        var event = start();
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        UUID sagaId = saga(event).getSagaId();
        runTwo(() -> retry.process(sagaId), () -> orchestrator.handleCreditScoringCompleted(scoring(event)));
        assertEquals(SagaStep.BUREAU, saga(event).getCurrentStep());
        assertEquals(1, outbox.findAll().stream()
                .filter(o -> o.getCommandType().equals("PerformBureauCheck")).count());
        assertTrue(attempts.count() <= 3); // 1 scoring + optional retry + 1 bureau.
    }

    @Test
    void rollbackOnOutboxInsertFailureAlsoRollsBackNewAttempt() {
        var event = start();
        publisher.publishPending();
        setClock(NOW.plusSeconds(3));
        doThrow(new IllegalStateException("outbox unavailable")).when(outboxSpy).save(any());
        assertThrows(IllegalStateException.class, () -> retry.process(saga(event).getSagaId()));
        assertEquals(1, attempts.count());
        assertEquals(1, outbox.count());
    }

    @Test
    void retryStateIsDatabaseDurableAcrossLaterSchedulerCalls() {
        var event = start();
        publisher.publishPending();
        var before = latest(event).getDeadlineAt();
        setClock(NOW.plusSeconds(1));
        scheduler.checkTimeouts();
        assertEquals(before, latest(event).getDeadlineAt());
        setClock(NOW.plusSeconds(3));
        scheduler.checkTimeouts();
        assertEquals(2, latest(event).getAttemptNumber());
        assertNull(latest(event).getPublishedAt());
        scheduler.checkTimeouts();
        assertEquals(2, attempts.count());
    }

    private ApplicationCreatedEvent start() {
        var event = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", NOW,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("3000.00"), 12, "EUR");
        orchestrator.initialize(event);
        return event;
    }

    private CreditScoringCompletedEvent scoring(ApplicationCreatedEvent application) {
        return new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", NOW,
                application.correlationId(), application.applicationId(), 750, true);
    }

    private SagaInstance saga(ApplicationCreatedEvent event) {
        return sagas.findAll().stream()
                .filter(row -> row.getApplicationId().equals(event.applicationId()))
                .findFirst().orElseThrow();
    }

    private SagaStepAttempt latest(ApplicationCreatedEvent event) {
        return attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                saga(event).getSagaId(), SagaStep.SCORING).orElseThrow();
    }

    private void runTwo(Runnable a, Runnable b) throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var first = pool.submit(() -> { start.await(); a.run(); return null; });
            var second = pool.submit(() -> { start.await(); b.run(); return null; });
            start.countDown();
            first.get(20, TimeUnit.SECONDS);
            second.get(20, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }
}
