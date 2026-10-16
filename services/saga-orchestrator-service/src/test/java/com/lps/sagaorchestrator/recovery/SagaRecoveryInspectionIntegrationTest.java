package com.lps.sagaorchestrator.recovery;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.event.LoanDisbursementCompletedEvent;
import com.lps.contracts.event.LoanNotificationCompletedEvent;
import com.lps.contracts.model.NotificationType;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import com.lps.sagaorchestrator.saga.SagaStep;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Real PostgreSQL verifies inspections never mutate the persisted Saga or Outbox state. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false",
        "lps.recovery.startup-scan-enabled=false"
})
class SagaRecoveryInspectionIntegrationTest {
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void db(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired SagaOrchestrationService service;
    @Autowired SagaRecoveryService recovery;
    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;
    @Autowired SagaStepAttemptRepository attempts;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }

    @Test
    void classifiesPersistedNewClaimedPublishedAndFailedWithoutNewCommands() {
        ApplicationCreatedEvent a = application();
        service.initialize(a);
        assertCount(SagaRecoveryClassification.WAITING_FOR_PUBLICATION, 1);

        UUID id = outbox.findAll().get(0).getId();
        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events
                SET status='CLAIMED', claimed_at=NOW() WHERE id=?
                """, id);
        assertCount(SagaRecoveryClassification.PUBLICATION_IN_PROGRESS, 1);

        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events
                SET status='PUBLISHED', claimed_at=NULL, published_at=NOW() WHERE id=?
                """, id);
        assertCount(SagaRecoveryClassification.AWAITING_COMPLETION, 1);

        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events
                SET status='FAILED', published_at=NULL WHERE id=?
                """, id);
        assertCount(SagaRecoveryClassification.PUBLICATION_FAILED, 1);
        assertEquals(1, outbox.count());
        assertEquals(1, sagas.count());
    }

    @Test
    void missingCurrentCommandIsDiagnosedNotRecreated() {
        ApplicationCreatedEvent a = application();
        service.initialize(a);
        service.handleCreditScoringCompleted(scoring(a));
        var bureau = outbox.findAll().stream().filter(c -> c.getCommandType().equals("PerformBureauCheck"))
                .findFirst().orElseThrow();
        outbox.deleteById(bureau.getId());
        assertCount(SagaRecoveryClassification.INCONSISTENT, 1);
        assertEquals(1, outbox.count()); // existing scoring command only
        assertEquals(1, sagas.count());
    }

    @Test
    void completedStateAndTimestampRemainImmutableAcrossMultipleInspections() {
        ApplicationCreatedEvent a = application();
        service.initialize(a);
        service.handleCreditScoringCompleted(scoring(a));
        service.handleBureauCheckCompleted(new BureauCheckCompletedEvent(
                UUID.randomUUID(), "BureauCheckCompleted", Instant.now(), a.correlationId(),
                a.applicationId(), 720, false, true));
        service.handleDecisionCompleted(new DecisionCompletedEvent(
                UUID.randomUUID(), "DecisionCompleted", Instant.now(), a.correlationId(),
                a.applicationId(), Decision.APPROVED, "verified"));
        // APPROVED is not terminal until payout and notification finish.
        UUID sagaId = sagas.findAll().get(0).getSagaId();
        UUID payoutCommandId = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, SagaStep.DISBURSEMENT).orElseThrow().getCommandId();
        service.handleDisbursementCompleted(new LoanDisbursementCompletedEvent(
                UUID.randomUUID(), "LoanDisbursementCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), payoutCommandId,
                UUID.randomUUID(), a.amount(), a.currency()));
        UUID notificationCommandId = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, SagaStep.NOTIFICATION).orElseThrow().getCommandId();
        service.handleNotificationCompleted(new LoanNotificationCompletedEvent(
                UUID.randomUUID(), "LoanNotificationCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), notificationCommandId,
                NotificationType.LOAN_APPROVED_AND_DISBURSED));

        Instant completedAt = sagas.findAll().get(0).getCompletedAt();
        assertNotNull(completedAt);
        assertEquals(0, recovery.inspect().inspected());
        assertEquals(0, recovery.inspect().inspected());
        assertEquals(completedAt, sagas.findAll().get(0).getCompletedAt());
        assertEquals(5, outbox.count());
    }

    @Test
    void concurrentInspectionsHaveNoBusinessSideEffects() throws Exception {
        ApplicationCreatedEvent a = application();
        service.initialize(a);
        var first = CompletableFuture.supplyAsync(recovery::inspect);
        var second = CompletableFuture.supplyAsync(recovery::inspect);
        assertEquals(1, first.get(15, TimeUnit.SECONDS).inspected());
        assertEquals(1, second.get(15, TimeUnit.SECONDS).inspected());
        assertEquals(1, outbox.count());
        assertEquals(1, sagas.count());
    }

    private void assertCount(SagaRecoveryClassification classification, long count) {
        assertEquals(count, recovery.inspect().count(classification));
    }

    private static ApplicationCreatedEvent application() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("9000.00"), 12, "EUR");
    }

    private static CreditScoringCompletedEvent scoring(ApplicationCreatedEvent app) {
        return new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                app.correlationId(), app.applicationId(), 750, true);
    }
}
