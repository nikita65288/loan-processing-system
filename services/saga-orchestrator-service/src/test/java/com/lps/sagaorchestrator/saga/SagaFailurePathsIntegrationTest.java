package com.lps.sagaorchestrator.saga;

import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.inbox.ProcessedMessageDao;
import com.lps.sagaorchestrator.kafka.SagaCommandProducer;
import com.lps.sagaorchestrator.outbox.SagaOutboxPublisher;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.retry.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
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

/** Actual PostgreSQL transactions prove failure handling, Inbox rollback and race safety. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false", "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false",
        "lps.saga.retry.max-attempts=2", "lps.saga.retry.backoff=1s",
        "lps.saga.timeout.scoring=2s", "lps.saga.timeout.bureau=2s", "lps.saga.timeout.decision=2s"
})
class SagaFailurePathsIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-10-12T10:00:00Z");
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired SagaOrchestrationService orchestrator;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired SagaRetryService retries;
    @Autowired SagaTimeoutScheduler scheduler;
    @Autowired JdbcTemplate jdbc;
    @Autowired SagaCompensationPolicy compensationPolicy;
    @MockitoBean Clock clock;
    @MockitoBean SagaCommandProducer producer;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
        when(clock.instant()).thenReturn(NOW);
        when(producer.publish(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void permanentScoringFailureIsTerminalAndNoBureauCommandExists() {
        var app = start();
        var failure = scoringFailure(app, false);
        orchestrator.handleFailure(failure, SagaStep.SCORING);
        var state = state(app);
        assertEquals(SagaStatus.FAILED, state.getStatus());
        assertEquals(SagaStep.SCORING, state.getCurrentStep());
        assertEquals("SCORING_INVALID_REQUEST", state.getFailureReason());
        assertEquals(failure.commandId(), state.getFailedCommandId());
        assertNotNull(state.getFailedAt());
        assertNull(state.getFinalDecision());
        assertEquals(1L, outbox.countByApplicationId(app.applicationId()));
        assertFalse(compensationPolicy.requiresCompensation(state));
        orchestrator.handleFailure(failure, SagaStep.SCORING);
        assertEquals(1L, outbox.countByApplicationId(app.applicationId()));
        orchestrator.handleCreditScoringCompleted(scoringCompleted(app));
        assertEquals(SagaStatus.FAILED, state(app).getStatus());
    }

    @Test
    void retryableFailureUsesOriginalDeadlineAndBoundedRetry() {
        var app = start();
        publisher.publishPending();
        var first = attempt(app, SagaStep.SCORING);
        assertNotNull(first.getPublishedAt());
        var failure = scoringFailure(app, true);
        orchestrator.handleFailure(failure, SagaStep.SCORING);
        orchestrator.handleFailure(failure, SagaStep.SCORING);
        assertEquals(SagaStatus.IN_PROGRESS, state(app).getStatus());
        assertEquals("SCORING_TEMPORARILY_UNAVAILABLE", state(app).getLastErrorCode());
        assertEquals(1L, outbox.countByApplicationId(app.applicationId()));
        when(clock.instant()).thenReturn(NOW.plusSeconds(2));
        scheduler.checkTimeouts(); // retryAfterAt = +3, so no early attempt
        assertEquals(1L, attempts.count());
        when(clock.instant()).thenReturn(NOW.plusSeconds(3));
        scheduler.checkTimeouts();
        assertEquals(2L, attempts.count());
        assertEquals(2L, outbox.countByApplicationId(app.applicationId()));
        assertEquals(first.getCommandId(), attempt(app, SagaStep.SCORING).getCommandId());
        publisher.publishPending();
        when(clock.instant()).thenReturn(NOW.plusSeconds(5));
        scheduler.checkTimeouts();
        assertEquals(SagaStatus.FAILED, state(app).getStatus());
        assertEquals("STEP_TIMEOUT", state(app).getFailureReason());
    }

    @Test
    void permanentBureauFailureKeepsScoringResultAndDoesNotEnqueueDecision() {
        var app = start();
        orchestrator.handleCreditScoringCompleted(scoringCompleted(app));
        var failure = new BureauCheckFailedEvent(UUID.randomUUID(), "BureauCheckFailed", NOW,
                app.correlationId(), app.applicationId(), attempt(app, SagaStep.BUREAU).getCommandId(),
                "BUREAU_INVALID_REQUEST", false);
        orchestrator.handleFailure(failure, SagaStep.BUREAU);
        assertEquals(750, state(app).getCreditScore());
        assertEquals(SagaStatus.FAILED, state(app).getStatus());
        assertEquals(SagaStep.BUREAU, state(app).getCurrentStep());
        assertEquals(2L, outbox.countByApplicationId(app.applicationId()));
        assertFalse(compensationPolicy.requiresCompensation(state(app)));
    }

    @Test
    void retryableBureauFailureKeepsExistingAttemptUntilSchedulerDeadline() {
        var app = start();
        orchestrator.handleCreditScoringCompleted(scoringCompleted(app));
        var event = new BureauCheckFailedEvent(UUID.randomUUID(), "BureauCheckFailed", NOW,
                app.correlationId(), app.applicationId(), attempt(app, SagaStep.BUREAU).getCommandId(),
                "BUREAU_TEMPORARILY_UNAVAILABLE", true);
        orchestrator.handleFailure(event, SagaStep.BUREAU);
        orchestrator.handleFailure(event, SagaStep.BUREAU);
        assertEquals(SagaStatus.IN_PROGRESS, state(app).getStatus());
        assertEquals(SagaStep.BUREAU, state(app).getCurrentStep());
        assertEquals("BUREAU_TEMPORARILY_UNAVAILABLE", state(app).getLastErrorCode());
        assertEquals(2L, outbox.countByApplicationId(app.applicationId()));
        assertEquals(2L, attempts.count());
        assertEquals(750, state(app).getCreditScore());
    }

    @Test
    void secondConflictingPermanentFailureCannotRewriteOriginalCause() {
        var app = start();
        var first = scoringFailure(app, false);
        orchestrator.handleFailure(first, SagaStep.SCORING);
        var second = new CreditScoringFailedEvent(UUID.randomUUID(), "CreditScoringFailed", NOW,
                app.correlationId(), app.applicationId(), first.commandId(),
                "SCORING_TEMPORARILY_UNAVAILABLE", true);
        orchestrator.handleFailure(second, SagaStep.SCORING);
        assertEquals("SCORING_INVALID_REQUEST", state(app).getFailureReason());
        assertEquals(first.commandId(), state(app).getFailedCommandId());
        assertEquals(SagaStatus.FAILED, state(app).getStatus());
    }

    @Test
    void concurrentPermanentFailureEventsStayTerminalAndDoNotDuplicateActions() throws Exception {
        var app = start();
        var first = scoringFailure(app, false);
        var second = scoringFailure(app, false);
        runConcurrently(() -> orchestrator.handleFailure(first, SagaStep.SCORING),
                () -> orchestrator.handleFailure(second, SagaStep.SCORING));
        assertEquals(SagaStatus.FAILED, state(app).getStatus());
        assertEquals(1L, outbox.countByApplicationId(app.applicationId()));
        assertEquals(1L, attempts.count());
    }

    @Test
    void permanentDecisionFailurePreservesEarlierResultsWithoutFakeRejection() {
        var app = start();
        orchestrator.handleCreditScoringCompleted(scoringCompleted(app));
        orchestrator.handleBureauCheckCompleted(bureauCompleted(app));
        var event = new DecisionFailedEvent(UUID.randomUUID(), "DecisionFailed", NOW,
                app.correlationId(), app.applicationId(), attempt(app, SagaStep.DECISION).getCommandId(),
                "DECISION_INVALID_REQUEST", false);
        orchestrator.handleFailure(event, SagaStep.DECISION);
        assertEquals(750, state(app).getCreditScore());
        assertEquals(720, state(app).getBureauScore());
        assertNull(state(app).getFinalDecision());
        assertEquals(SagaStatus.FAILED, state(app).getStatus());
        assertEquals(3L, outbox.countByApplicationId(app.applicationId()));
    }

    @Test
    void businessRejectionCompletesWithoutTechnicalFailure() {
        var app = start();
        orchestrator.handleCreditScoringCompleted(scoringCompleted(app));
        orchestrator.handleBureauCheckCompleted(bureauCompleted(app));
        orchestrator.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted",
                NOW, app.correlationId(), app.applicationId(), Decision.REJECTED, "FAIL_RULES"));
        var state = state(app);
        assertEquals(SagaStep.NOTIFICATION,state.getCurrentStep());
        assertEquals(SagaStatus.IN_PROGRESS,state.getStatus());
        var noteAttempt=attempt(app,SagaStep.NOTIFICATION);
        orchestrator.handleNotificationCompleted(new LoanNotificationCompletedEvent(UUID.randomUUID(),
                "LoanNotificationCompleted",NOW,app.correlationId(),app.applicationId(),
                noteAttempt.getCommandId(),com.lps.contracts.model.NotificationType.LOAN_REJECTED));
        state=state(app);
        assertEquals(SagaStatus.COMPLETED, state.getStatus());
        assertEquals(SagaStep.FINISHED, state.getCurrentStep());
        assertEquals(Decision.REJECTED, state.getFinalDecision());
        assertNull(state.getFailureReason());
        assertNull(state.getFailedAt());
        assertFalse(compensationPolicy.requiresCompensation(state));
        orchestrator.handleFailure(new DecisionFailedEvent(UUID.randomUUID(), "DecisionFailed", NOW,
                app.correlationId(), app.applicationId(), attempt(app, SagaStep.DECISION).getCommandId(),
                "DECISION_INVALID_REQUEST", false), SagaStep.DECISION);
        assertEquals(SagaStatus.COMPLETED, state(app).getStatus());
    }

    @Test
    void invalidCommandRollsBackInboxAndState() {
        var app = start();
        var event = new CreditScoringFailedEvent(UUID.randomUUID(), "CreditScoringFailed", NOW,
                app.correlationId(), app.applicationId(), UUID.randomUUID(), "SCORING_INVALID_REQUEST", false);
        assertThrows(IllegalStateException.class, () -> orchestrator.handleFailure(event, SagaStep.SCORING));
        Integer registered = jdbc.queryForObject("SELECT count(*) FROM saga_orchestrator.processed_messages WHERE message_id=?",
                Integer.class, event.eventId());
        assertEquals(0, registered);
        assertEquals(SagaStatus.IN_PROGRESS, state(app).getStatus());
    }

    @Test
    void successAndPermanentFailureRaceCannotOverwriteCommittedResult() throws Exception {
        var app = start();
        orchestrator.handleCreditScoringCompleted(scoringCompleted(app));
        var failure = new BureauCheckFailedEvent(UUID.randomUUID(), "BureauCheckFailed", NOW,
                app.correlationId(), app.applicationId(), attempt(app, SagaStep.BUREAU).getCommandId(),
                "BUREAU_INVALID_REQUEST", false);
        runConcurrently(() -> orchestrator.handleFailure(failure, SagaStep.BUREAU),
                () -> orchestrator.handleBureauCheckCompleted(bureauCompleted(app)));
        var state = state(app);
        assertTrue(state.getStatus() == SagaStatus.FAILED || state.getCurrentStep() == SagaStep.DECISION);
        assertTrue(outbox.countByApplicationId(app.applicationId()) == (state.getStatus() == SagaStatus.FAILED ? 2 : 3));
    }

    @Test
    void retryableFailureAndTimeoutRaceDoesNotDuplicateAttempt() throws Exception {
        var app = start();
        publisher.publishPending();
        when(clock.instant()).thenReturn(NOW.plusSeconds(3));
        runConcurrently(() -> orchestrator.handleFailure(scoringFailure(app, true), SagaStep.SCORING),
                () -> retries.process(state(app).getSagaId()));
        assertEquals(2L, attempts.count());
        assertEquals(2L, outbox.countByApplicationId(app.applicationId()));
    }

    private void runConcurrently(Runnable a, Runnable b) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(1);
        try {
            Future<?> first = executor.submit(() -> { await(latch); a.run(); });
            Future<?> second = executor.submit(() -> { await(latch); b.run(); });
            latch.countDown();
            first.get(15, TimeUnit.SECONDS);
            second.get(15, TimeUnit.SECONDS);
        } finally { executor.shutdownNow(); }
    }

    private static void await(CountDownLatch latch) {
        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
    }

    private ApplicationCreatedEvent start() {
        var event = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", NOW,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("5000"), 12, "EUR");
        orchestrator.initialize(event);
        return event;
    }

    private SagaInstance state(ApplicationCreatedEvent app) {
        return sagas.findAll().stream().filter(s -> s.getApplicationId().equals(app.applicationId())).findFirst().orElseThrow();
    }

    private SagaStepAttempt attempt(ApplicationCreatedEvent app, SagaStep step) {
        return attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(state(app).getSagaId(), step).orElseThrow();
    }

    private CreditScoringFailedEvent scoringFailure(ApplicationCreatedEvent app, boolean retryable) {
        return new CreditScoringFailedEvent(UUID.randomUUID(), "CreditScoringFailed", NOW,
                app.correlationId(), app.applicationId(), attempt(app, SagaStep.SCORING).getCommandId(),
                retryable ? "SCORING_TEMPORARILY_UNAVAILABLE" : "SCORING_INVALID_REQUEST", retryable);
    }

    private CreditScoringCompletedEvent scoringCompleted(ApplicationCreatedEvent app) {
        return new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", NOW,
                app.correlationId(), app.applicationId(), 750, true);
    }

    private BureauCheckCompletedEvent bureauCompleted(ApplicationCreatedEvent app) {
        return new BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted", NOW,
                app.correlationId(), app.applicationId(), 720, false, true);
    }
}
