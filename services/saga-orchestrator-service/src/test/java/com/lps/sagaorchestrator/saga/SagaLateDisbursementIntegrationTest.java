package com.lps.sagaorchestrator.saga;

import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.contracts.model.NotificationType;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.retry.SagaRetryService;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

/** PostgreSQL integration tests exercise actual timeout transitions, transaction rollback and Saga row locks. */
@Testcontainers
@SpringBootTest(properties = {"lps.scheduling.enabled=false", "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false", "lps.saga.retry.max-attempts=1"})
class SagaLateDisbursementIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url", DB::getJdbcUrl);
        props.add("spring.datasource.username", DB::getUsername);
        props.add("spring.datasource.password", DB::getPassword);
    }

    @Autowired SagaOrchestrationService service;
    @Autowired SagaRetryService retries;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired SagaOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }

    /** Build the real approved workflow, including the original durable payout command. */
    private ApplicationCreatedEvent approved() {
        UUID applicationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        Instant now = Instant.now();
        var creation = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", now,
                correlationId, applicationId, customerId, new BigDecimal("8000.00"), 12, "EUR");
        service.initialize(creation);
        service.handleCreditScoringCompleted(new CreditScoringCompletedEvent(UUID.randomUUID(),
                "CreditScoringCompleted", now, correlationId, applicationId, 760, true));
        service.handleBureauCheckCompleted(new BureauCheckCompletedEvent(UUID.randomUUID(),
                "BureauCheckCompleted", now, correlationId, applicationId, 740, false, true));
        service.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),
                "DecisionCompleted", now, correlationId, applicationId, Decision.APPROVED, "PASS"));
        assertEquals(SagaStep.DISBURSEMENT, state(creation).getCurrentStep());
        return creation;
    }

    private SagaInstance state(ApplicationCreatedEvent application) {
        return sagas.findByApplicationId(application.applicationId()).orElseThrow();
    }

    private UUID payoutCommandId(ApplicationCreatedEvent application) {
        return attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                state(application).getSagaId(), SagaStep.DISBURSEMENT).orElseThrow().getCommandId();
    }

    /** Activate the persisted deadline and let the production retry policy mark UNKNOWN. */
    private void timeOutPayout(ApplicationCreatedEvent application) {
        Instant expired = Instant.now().minusSeconds(120);

        jdbc.update("""
            UPDATE saga_orchestrator.saga_step_attempts
            SET published_at=?, deadline_at=?, retry_after_at=?
            WHERE saga_id=? AND step='DISBURSEMENT'
            """,
                Timestamp.from(expired.minusSeconds(90)),
                Timestamp.from(expired),
                Timestamp.from(expired.plusSeconds(5)),
                state(application).getSagaId());

        retries.process(state(application).getSagaId());

        assertEquals(SagaStatus.REQUIRES_RECONCILIATION, state(application).getStatus());
        assertEquals("DISBURSEMENT_UNKNOWN", state(application).getFailureReason());
    }

    private LoanDisbursementCompletedEvent result(ApplicationCreatedEvent application, UUID eventId, UUID commandId,
                                                  UUID disbursementId) {
        return new LoanDisbursementCompletedEvent(eventId, "LoanDisbursementCompleted",
                Instant.now(), application.correlationId(), application.applicationId(), commandId,
                disbursementId, application.amount(), application.currency());
    }

    private long commands(ApplicationCreatedEvent application, String type) {
        return outbox.findAll().stream().filter(e -> e.getApplicationId().equals(application.applicationId()))
                .filter(e -> type.equals(e.getCommandType())).count();
    }

    @Test
    void lateConfirmationResumesNotificationThenCompletesNormally() {
        var application = approved();
        UUID commandId = payoutCommandId(application);
        UUID disbursementId = UUID.randomUUID();
        timeOutPayout(application);

        var confirmed = result(application, UUID.randomUUID(), commandId, disbursementId);
        service.handleDisbursementCompleted(confirmed);
        var resumed = state(application);
        assertEquals(SagaStatus.IN_PROGRESS, resumed.getStatus());
        assertEquals(SagaStep.NOTIFICATION, resumed.getCurrentStep());
        assertNull(resumed.getFailureReason());
        assertNull(resumed.getFailedAt());
        assertEquals("COMPLETED", resumed.getDisbursementStatus());
        assertEquals(disbursementId, resumed.getDisbursementId());
        assertEquals(1, commands(application, "DisburseLoan"));
        assertEquals(1, commands(application, "SendLoanDecisionNotification"));

        // The same delivery and a different eventId for the same business outcome are no-ops.
        service.handleDisbursementCompleted(confirmed);
        service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, disbursementId));
        retries.process(resumed.getSagaId());
        assertEquals(1, commands(application, "SendLoanDecisionNotification"));
        UUID notificationCommandId = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                resumed.getSagaId(), SagaStep.NOTIFICATION).orElseThrow().getCommandId();
        service.handleNotificationCompleted(new LoanNotificationCompletedEvent(UUID.randomUUID(),
                "LoanNotificationCompleted", Instant.now(), application.correlationId(),
                application.applicationId(), notificationCommandId, NotificationType.LOAN_APPROVED_AND_DISBURSED));
        assertEquals(SagaStatus.COMPLETED, state(application).getStatus());
        service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, disbursementId));
        assertEquals(SagaStatus.COMPLETED, state(application).getStatus());
        assertEquals(1, commands(application, "SendLoanDecisionNotification"));
    }

    @Test
    void wrongCommandIdRollsBackInboxAndCannotRecoverUnknown() {
        var application = approved();
        timeOutPayout(application);
        UUID messageId = UUID.randomUUID();
        assertThrows(IllegalStateException.class, () -> service.handleDisbursementCompleted(
                result(application, messageId, UUID.randomUUID(), UUID.randomUUID())));
        assertEquals(SagaStatus.REQUIRES_RECONCILIATION, state(application).getStatus());
        assertEquals(0, jdbc.queryForObject("""
                SELECT count(*) FROM saga_orchestrator.processed_messages WHERE message_id=?
                """, Integer.class, messageId).intValue());
        assertEquals(0, commands(application, "SendLoanDecisionNotification"));
    }

    @Test
    void differentPayoutIdForSameOperationIsNotAValidSemanticDuplicate() {
        var application = approved();
        UUID commandId = payoutCommandId(application);
        UUID firstPayoutId = UUID.randomUUID();
        service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, firstPayoutId));
        UUID conflictingEventId = UUID.randomUUID();
        assertThrows(IllegalStateException.class, () -> service.handleDisbursementCompleted(
                result(application, conflictingEventId, commandId, UUID.randomUUID())));
        assertEquals(firstPayoutId, state(application).getDisbursementId());
        assertEquals(1, commands(application, "SendLoanDecisionNotification"));
        assertEquals(0, jdbc.queryForObject("""
                SELECT count(*) FROM saga_orchestrator.processed_messages WHERE message_id=?
                """, Integer.class, conflictingEventId).intValue());
    }

    @Test
    void unrelatedReconciliationReasonIsNotAutomaticallyCleared() {
        var application = approved();
        timeOutPayout(application);
        jdbc.update("UPDATE saga_orchestrator.saga_instances SET failure_reason='OTHER_UNKNOWN' WHERE application_id=?",
                application.applicationId());
        service.handleDisbursementCompleted(result(application, UUID.randomUUID(),
                payoutCommandId(application), UUID.randomUUID()));
        assertEquals(SagaStatus.REQUIRES_RECONCILIATION, state(application).getStatus());
        assertEquals("OTHER_UNKNOWN", state(application).getFailureReason());
        assertEquals(0, commands(application, "SendLoanDecisionNotification"));
    }

    @Test
    void permanentDisbursementFailureIsNotReopenedByLateSuccess() {
        var application = approved();
        UUID commandId = payoutCommandId(application);
        service.handleFailure(new LoanDisbursementFailedEvent(UUID.randomUUID(), "LoanDisbursementFailed",
                Instant.now(), application.correlationId(), application.applicationId(), commandId,
                "PERMANENT_ERROR", false), SagaStep.DISBURSEMENT);
        assertEquals(SagaStatus.FAILED, state(application).getStatus());
        service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, UUID.randomUUID()));
        assertEquals(SagaStatus.FAILED, state(application).getStatus());
        assertEquals(0, commands(application, "SendLoanDecisionNotification"));
    }

    @Test
    void completionRacingWithTimeoutNeverLeavesPayoutUnknown() throws Exception {
        var application = approved();
        UUID commandId = payoutCommandId(application);
        UUID payoutId = UUID.randomUUID();
        Instant expired = Instant.now().minusSeconds(120);
        jdbc.update("""
                UPDATE saga_orchestrator.saga_step_attempts
                SET published_at=?, deadline_at=?, retry_after_at=?
                WHERE saga_id=? AND step='DISBURSEMENT'
                """,
                    Timestamp.from(expired.minusSeconds(60)),
                    Timestamp.from(expired),
                    Timestamp.from(expired.plusSeconds(5)),
                    state(application).getSagaId());

        var pool = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var completion = pool.submit(() -> {
                start.await();
                service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, payoutId));
                return true;
            });
            var timeout = pool.submit(() -> {
                start.await();
                retries.process(state(application).getSagaId());
                return true;
            });
            start.countDown();
            assertTrue(completion.get(20, TimeUnit.SECONDS));
            assertTrue(timeout.get(20, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals(SagaStatus.IN_PROGRESS, state(application).getStatus());
        assertEquals(SagaStep.NOTIFICATION, state(application).getCurrentStep());
        assertEquals(1, commands(application, "SendLoanDecisionNotification"));
        assertEquals(1, commands(application, "DisburseLoan"));
    }

    @Test
    void parallelDifferentEventIdsApplyOneNotificationTransition() throws Exception {
        var application = approved();
        timeOutPayout(application);
        UUID commandId = payoutCommandId(application);
        UUID payoutId = UUID.randomUUID();
        var pool = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var first = pool.submit(() -> {
                start.await();
                service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, payoutId));
                return true;
            });
            var second = pool.submit(() -> {
                start.await();
                service.handleDisbursementCompleted(result(application, UUID.randomUUID(), commandId, payoutId));
                return true;
            });
            start.countDown();
            assertTrue(first.get(20, TimeUnit.SECONDS));
            assertTrue(second.get(20, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals(SagaStatus.IN_PROGRESS, state(application).getStatus());
        assertEquals(SagaStep.NOTIFICATION, state(application).getCurrentStep());
        assertEquals(1, commands(application, "SendLoanDecisionNotification"));
    }
}
