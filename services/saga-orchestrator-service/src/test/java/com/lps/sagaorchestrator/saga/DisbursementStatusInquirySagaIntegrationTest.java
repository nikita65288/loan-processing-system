package com.lps.sagaorchestrator.saga;

import com.lps.contracts.command.DisbursementStatusInquiryCommand;
import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.kafka.SagaKafkaConfiguration;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.retry.DisbursementStatusInquiryService;
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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Actual Saga locks, Inbox, Outbox and retry metadata under PostgreSQL transactions. */
@Testcontainers
@SpringBootTest(properties={"lps.scheduling.enabled=false", "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false", "lps.saga.retry.max-attempts=1",
        "lps.saga.inquiry.retry-interval=1s", "lps.saga.inquiry.max-attempts=2"})
class DisbursementStatusInquirySagaIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource static void db(DynamicPropertyRegistry props) {
        props.add("spring.datasource.url", DB::getJdbcUrl);
        props.add("spring.datasource.username", DB::getUsername);
        props.add("spring.datasource.password", DB::getPassword);
    }

    @Autowired SagaOrchestrationService orchestration;
    @Autowired DisbursementStatusInquiryService inquiries;
    @Autowired SagaRetryService retries;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository steps;
    @Autowired SagaOutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void clean() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }

    private ApplicationCreatedEvent approved() {
        UUID app = UUID.randomUUID(), corr = UUID.randomUUID();
        Instant now = Instant.now();
        var event = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", now,
                corr, app, UUID.randomUUID(), new BigDecimal("4000.00"), 12, "EUR");
        orchestration.initialize(event);
        orchestration.handleCreditScoringCompleted(new CreditScoringCompletedEvent(UUID.randomUUID(),
                "CreditScoringCompleted", now, corr, app, 750, true));
        orchestration.handleBureauCheckCompleted(new BureauCheckCompletedEvent(UUID.randomUUID(),
                "BureauCheckCompleted", now, corr, app, 740, false, true));
        orchestration.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),
                "DecisionCompleted", now, corr, app, Decision.APPROVED, "PASS"));
        return event;
    }

    private SagaInstance state(ApplicationCreatedEvent event) {
        return sagas.findByApplicationId(event.applicationId()).orElseThrow();
    }

    private UUID commandId(ApplicationCreatedEvent event) {
        return steps.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                state(event).getSagaId(), SagaStep.DISBURSEMENT).orElseThrow().getCommandId();
    }

    /** Uses the production timeout transition, not a synthetic direct UPDATE to Saga state. */
    private void unknown(ApplicationCreatedEvent event) {
        Instant expired = Instant.now().minusSeconds(120);
        jdbc.update("""
                UPDATE saga_orchestrator.saga_step_attempts
                SET published_at=?, deadline_at=?, retry_after_at=?
                WHERE saga_id=? AND step='DISBURSEMENT'
                """, Timestamp.from(expired.minusSeconds(90)), Timestamp.from(expired),
                Timestamp.from(expired.plusSeconds(5)), state(event).getSagaId());
        retries.process(state(event).getSagaId());
        assertTrue(state(event).awaitsLateDisbursementConfirmation());
    }

    private DisbursementStatusInquiryCommand request(ApplicationCreatedEvent event) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                .readValue(outbox.findAll().stream()
                        .filter(x -> x.getTopic().equals(SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_REQUESTED_TOPIC))
                        .findFirst().orElseThrow().getPayload(), DisbursementStatusInquiryCommand.class);
    }

    private DisbursementStatusInquiryResultEvent reply(ApplicationCreatedEvent event,
            UUID inquiryId, DisbursementInquiryStatus status, UUID payoutId) {
        return new DisbursementStatusInquiryResultEvent(
                com.lps.contracts.MessageIdentity.resultEventId(inquiryId, "DisbursementStatusInquiryResult"),
                "DisbursementStatusInquiryResult",
                Instant.now(), event.correlationId(), event.applicationId(), inquiryId, commandId(event), status,
                payoutId, status == DisbursementInquiryStatus.COMPLETED ? event.amount() : null,
                status == DisbursementInquiryStatus.COMPLETED ? event.currency() : null);
    }

    private long notificationCommands(ApplicationCreatedEvent event) {
        return outbox.findAll().stream().filter(e -> e.getApplicationId().equals(event.applicationId()))
                .filter(e -> "SendLoanDecisionNotification".equals(e.getCommandType())).count();
    }

    @Test void statusSuccessResumesRealUnknownSagaWithoutAnotherPayoutOrNotification() throws Exception {
        var event = approved(); unknown(event);
        inquiries.initiate(state(event).getSagaId());
        var request = request(event);
        assertEquals(commandId(event), request.disbursementCommandId());
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM saga_orchestrator.disbursement_inquiry_attempts", Integer.class));
        UUID payoutId=UUID.randomUUID();
        var result = reply(event, request.inquiryId(), DisbursementInquiryStatus.COMPLETED, payoutId);
        orchestration.handleDisbursementInquiryResult(result);
        orchestration.handleDisbursementInquiryResult(result);
        assertEquals(SagaStep.NOTIFICATION, state(event).getCurrentStep());
        assertEquals(SagaStatus.IN_PROGRESS, state(event).getStatus());
        assertEquals(payoutId, state(event).getDisbursementId());
        assertEquals(1, notificationCommands(event));
        assertEquals(1, outbox.findAll().stream().filter(x -> "DisburseLoan".equals(x.getCommandType())).count());
        inquiries.initiate(state(event).getSagaId());
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM saga_orchestrator.disbursement_inquiry_attempts", Integer.class));
    }

    @Test void notFoundAndMismatchingIdentityNeverCreateNewPayout() throws Exception {
        var event = approved(); unknown(event);
        inquiries.initiate(state(event).getSagaId());
        var request = request(event);
        orchestration.handleDisbursementInquiryResult(reply(event, request.inquiryId(),
                DisbursementInquiryStatus.NOT_FOUND, null));
        orchestration.handleDisbursementInquiryResult(reply(event, UUID.randomUUID(),
                DisbursementInquiryStatus.COMPLETED, UUID.randomUUID()));
        assertTrue(state(event).awaitsLateDisbursementConfirmation());
        assertEquals(0, notificationCommands(event));
    }

    @Test void lateCompletionAndInquiryRaceProduceOneNotification() throws Exception {
        var event = approved(); unknown(event);
        inquiries.initiate(state(event).getSagaId());
        UUID requestId=request(event).inquiryId(), payout=UUID.randomUUID();
        var completed = new LoanDisbursementCompletedEvent(UUID.randomUUID(), "LoanDisbursementCompleted",
                Instant.now(), event.correlationId(), event.applicationId(), commandId(event), payout,
                event.amount(), event.currency());
        var pool=Executors.newFixedThreadPool(2);
        var latch=new CountDownLatch(1);
        try {
            var a=pool.submit(() -> { latch.await(); orchestration.handleDisbursementCompleted(completed); return true; });
            var b=pool.submit(() -> { latch.await(); orchestration.handleDisbursementInquiryResult(
                    reply(event, requestId, DisbursementInquiryStatus.COMPLETED, payout)); return true; });
            latch.countDown();
            assertTrue(a.get(20,TimeUnit.SECONDS));
            assertTrue(b.get(20,TimeUnit.SECONDS));
            assertEquals(1, notificationCommands(event));
        } finally { pool.shutdownNow(); }
    }

    /** A status lookup can race with the original payout commit: NOT_FOUND is not final. */
    @Test void missingCompletionNotFoundThenConfirmedSuccessRecoversWithoutSecondPayout() throws Exception {
        var event = approved();
        UUID originalCommandId = commandId(event);
        // The ordinary LoanDisbursementCompletedEvent is deliberately never delivered.
        unknown(event);
        UUID sagaId = state(event).getSagaId();
        inquiries.initiate(sagaId);
        UUID firstInquiryId = request(event).inquiryId();

        orchestration.handleDisbursementInquiryResult(
                reply(event, firstInquiryId, DisbursementInquiryStatus.NOT_FOUND, null));
        assertTrue(state(event).awaitsLateDisbursementConfirmation());
        assertEquals(0, notificationCommands(event));
        assertEquals(1, outbox.findAll().stream()
                .filter(x -> "DisburseLoan".equals(x.getCommandType()))
                .filter(x -> event.applicationId().equals(x.getApplicationId())).count());

        // A retry is a NEW inquiry: the same inquiryId has one immutable durable reply.
        // Simulate the prior request being published and its retry interval expiring.
        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events
                SET status='PUBLISHED', published_at=now(), claimed_at=NULL
                WHERE id=?
                """, firstInquiryId);
        jdbc.update("""
                UPDATE saga_orchestrator.disbursement_inquiry_attempts
                SET created_at=now()-INTERVAL '1 minute'
                WHERE inquiry_id=?
                """, firstInquiryId);
        inquiries.initiate(sagaId);
        UUID secondInquiryId = jdbc.queryForObject("""
                SELECT inquiry_id FROM saga_orchestrator.disbursement_inquiry_attempts
                WHERE saga_id=? AND attempt_number=2
                """, UUID.class, sagaId);
        assertNotNull(secondInquiryId);
        assertNotEquals(firstInquiryId, secondInquiryId);
        assertEquals(originalCommandId, jdbc.queryForObject("""
                SELECT disbursement_command_id FROM saga_orchestrator.disbursement_inquiry_attempts
                WHERE inquiry_id=?
                """, UUID.class, secondInquiryId));

        // The second lookup can now report the previously committed payout.
        UUID existingPayoutId = UUID.randomUUID();
        var success = reply(event, secondInquiryId,
                DisbursementInquiryStatus.COMPLETED, existingPayoutId);
        orchestration.handleDisbursementInquiryResult(success);
        orchestration.handleDisbursementInquiryResult(success);

        assertEquals(SagaStep.NOTIFICATION, state(event).getCurrentStep());
        assertEquals(existingPayoutId, state(event).getDisbursementId());
        assertEquals(1, notificationCommands(event));
        assertEquals(originalCommandId, commandId(event));
        assertEquals(1, outbox.findAll().stream()
                .filter(x -> "DisburseLoan".equals(x.getCommandType()))
                .filter(x -> event.applicationId().equals(x.getApplicationId())).count());
    }

    @Test void twoPublishedAttemptsExhaustPolicyWithoutNewDisbursement() throws Exception {
        var event = approved(); unknown(event);
        UUID sagaId=state(event).getSagaId();
        inquiries.initiate(sagaId);
        var original=request(event);
        jdbc.update("""
                UPDATE saga_orchestrator.disbursement_inquiry_attempts
                SET created_at=now()-INTERVAL '1 minute' WHERE inquiry_id=?
                """,original.inquiryId());
        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events
                SET status='PUBLISHED', published_at=now(),claimed_at=NULL
                WHERE id=?
                """,original.inquiryId());
        inquiries.initiate(sagaId);
        inquiries.initiate(sagaId);
        assertEquals(2,jdbc.queryForObject(
                "SELECT count(*) FROM saga_orchestrator.disbursement_inquiry_attempts",Integer.class));
        assertEquals(1,outbox.findAll().stream().filter(x -> "DisburseLoan".equals(x.getCommandType())).count());
        assertTrue(state(event).awaitsLateDisbursementConfirmation());
    }

    @Test void twoReplicasInitiatingTheSameRequestCreateOneDurableAttempt() throws Exception {
        var event=approved(); unknown(event);
        var pool=Executors.newFixedThreadPool(2);
        var latch=new CountDownLatch(1);
        try {
            var a=pool.submit(() -> { latch.await(); inquiries.initiate(state(event).getSagaId()); return true; });
            var b=pool.submit(() -> { latch.await(); inquiries.initiate(state(event).getSagaId()); return true; });
            latch.countDown();
            assertTrue(a.get(20,TimeUnit.SECONDS));
            assertTrue(b.get(20,TimeUnit.SECONDS));
            assertEquals(1, jdbc.queryForObject(
                    "SELECT count(*) FROM saga_orchestrator.disbursement_inquiry_attempts", Integer.class));
        } finally { pool.shutdownNow(); }
    }
}
