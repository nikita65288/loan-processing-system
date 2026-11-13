package com.lps.sagaorchestrator.outbox;

import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import com.lps.sagaorchestrator.kafka.SagaCommandProducer;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Uses real Outbox rows while controlling broker acknowledgement outcomes. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false",
        "lps.outbox.max-retries=3",
        "lps.outbox.kafka-publish-timeout-seconds=1"
})
class SagaOutboxPublisherIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configuration(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired SagaOrchestrationService service;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired MeterRegistry meters;
    @MockitoBean SagaCommandProducer producer;

    @BeforeEach
    void clean() {
        outbox.deleteAll();
    }

    @Test
    void publishedOnlyAfterSuccessfulKafkaAcknowledgementAndUsesApplicationKey() {
        long beforeAck = timerCount("lps.outbox.kafka.ack.duration");
        long beforeConfirmation = timerCount("lps.outbox.database.confirm.duration");
        long beforeRecovery = timerCount("lps.outbox.stale.claim.scan.duration");
        long beforeClaim = timerCount("lps.outbox.claim.batch.duration");
        long beforePoll = timerCount("lps.outbox.poll.cycle.duration");
        long beforeBatch = meters.get("lps.outbox.claim.batch.size").summary().count();
        double beforeClaimed = meters.get("lps.outbox.claim.batch.size").summary().totalAmount();
        ApplicationCreatedEvent event = event();
        service.initialize(event);
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        var saved = outbox.findAll().get(0);
        assertEquals(SagaOutboxStatus.PUBLISHED, saved.getStatus());
        assertNotNull(saved.getPublishedAt());
        var attempt = attempts.findByOutboxEventId(saved.getId()).orElseThrow();
        assertNotNull(attempt.getPublishedAt());
        assertNotNull(attempt.getDeadlineAt());
        assertEquals(attempt.getPublishedAt().plusSeconds(30), attempt.getDeadlineAt());
        long backoffSeconds = jdbc.queryForObject("""
                SELECT backoff_seconds FROM saga_orchestrator.saga_step_attempts
                WHERE outbox_event_id = ?
                """, Long.class, saved.getId());
        assertEquals(attempt.getDeadlineAt().plusSeconds(backoffSeconds), attempt.getRetryAfterAt());
        ArgumentCaptor<PerformCreditScoringCommand> captor =
                ArgumentCaptor.forClass(PerformCreditScoringCommand.class);
        verify(producer).publish(eq("loan.scoring.requested"), captor.capture());
        assertEquals(saved.getId(), captor.getValue().commandId());
        assertEquals(event.applicationId(), captor.getValue().applicationId());
        // Timers cover the real Kafka acknowledgement and the committed PostgreSQL confirmation.
        assertEquals(beforeAck + 1, timerCount("lps.outbox.kafka.ack.duration"));
        assertEquals(beforeConfirmation + 1, timerCount("lps.outbox.database.confirm.duration"));
        assertEquals(beforeRecovery + 1, timerCount("lps.outbox.stale.claim.scan.duration"));
        assertEquals(beforeClaim + 1, timerCount("lps.outbox.claim.batch.duration"));
        assertEquals(beforePoll + 1, timerCount("lps.outbox.poll.cycle.duration"));
        assertEquals(beforeBatch + 1, meters.get("lps.outbox.claim.batch.size").summary().count());
        assertEquals(beforeClaimed + 1.0,
                meters.get("lps.outbox.claim.batch.size").summary().totalAmount(), 0.001);
    }

    @Test
    void failedSendDoesNotLoseCommandAndRetryKeepsSameCommandId() {
        long beforeAck = timerCount("lps.outbox.kafka.ack.duration");
        long beforeConfirmation = timerCount("lps.outbox.database.confirm.duration");
        ApplicationCreatedEvent event = event();
        service.initialize(event);
        UUID commandId = outbox.findAll().get(0).getId();
        CompletableFuture fail = new CompletableFuture();
        fail.completeExceptionally(new IllegalStateException("Kafka unavailable"));
        when(producer.publish(anyString(), any())).thenReturn(fail)
                .thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        assertEquals(SagaOutboxStatus.NEW, outbox.findById(commandId).orElseThrow().getStatus());
        assertEquals(1, outbox.findById(commandId).orElseThrow().getRetryCount());
        assertNull(attempts.findByOutboxEventId(commandId).orElseThrow().getDeadlineAt());
        publisher.publishPending();
        assertEquals(SagaOutboxStatus.PUBLISHED, outbox.findById(commandId).orElseThrow().getStatus());
        ArgumentCaptor<PerformCreditScoringCommand> captor =
                ArgumentCaptor.forClass(PerformCreditScoringCommand.class);
        verify(producer, times(2)).publish(eq("loan.scoring.requested"), captor.capture());
        assertTrue(captor.getAllValues().stream().allMatch(c -> c.commandId().equals(commandId)));
        // Failed acknowledgements count as attempted Kafka IO, but never as DB confirmations.
        assertEquals(beforeAck + 2, timerCount("lps.outbox.kafka.ack.duration"));
        assertEquals(beforeConfirmation + 1, timerCount("lps.outbox.database.confirm.duration"));
    }

    @Test
    void kafkaTimeoutRecordsAckWaitButNeverDatabaseConfirmation() {
        long beforeAck = timerCount("lps.outbox.kafka.ack.duration");
        long beforeConfirmation = timerCount("lps.outbox.database.confirm.duration");
        ApplicationCreatedEvent application = event();
        service.initialize(application);
        when(producer.publish(anyString(), any())).thenReturn(new CompletableFuture<>());

        publisher.publishPending(); // Test profile sets the Kafka acknowledgement timeout to one second.

        SagaOutboxEvent row = outbox.findAll().get(0);
        assertEquals(SagaOutboxStatus.NEW, row.getStatus());
        assertEquals(1, row.getRetryCount());
        assertNull(row.getPublishedAt());
        assertEquals(beforeAck + 1, timerCount("lps.outbox.kafka.ack.duration"));
        assertEquals(beforeConfirmation, timerCount("lps.outbox.database.confirm.duration"));
        verify(producer, times(1)).publish(anyString(), any());
    }

    @Test
    void twoConcurrentPublishersClaimAnOutboxRowOnlyOnce() throws Exception {
        service.initialize(event());
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Runnable task = () -> {
                try {
                    start.await();
                    publisher.publishPending();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            };
            Future<?> a = pool.submit(task);
            Future<?> b = pool.submit(task);
            start.countDown();
            a.get(20, TimeUnit.SECONDS);
            b.get(20, TimeUnit.SECONDS);
            assertEquals(SagaOutboxStatus.PUBLISHED, outbox.findAll().get(0).getStatus());
            verify(producer, times(1)).publish(eq("loan.scoring.requested"), any());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void staleClaimIsPublishedAfterLeaseExpiryWithOriginalCommandId() {
        ApplicationCreatedEvent application = event();
        service.initialize(application);
        UUID commandId = outbox.findAll().get(0).getId();
        jdbc.update("""
                UPDATE saga_orchestrator.saga_outbox_events
                SET status='CLAIMED', claimed_at=NOW() - INTERVAL '1 day'
                WHERE id=?
                """, commandId);
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPending(); // Existing stale-claim recovery, then normal claiming.

        var persisted = outbox.findById(commandId).orElseThrow();
        assertEquals(SagaOutboxStatus.PUBLISHED, persisted.getStatus());
        assertEquals(commandId, persisted.getId());
        verify(producer, times(1)).publish(eq("loan.scoring.requested"), any());
        assertEquals(1, outbox.countByApplicationId(application.applicationId()));
    }

    @Test
    void alreadyPublishedCommandIsNotSentAgainAfterPublisherRestart() {
        ApplicationCreatedEvent application = event();
        service.initialize(application);
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        publisher.publishPending(); // Equivalent to a second polling cycle after restart.
        assertEquals(SagaOutboxStatus.PUBLISHED, outbox.findAll().get(0).getStatus());
        verify(producer, times(1)).publish(eq("loan.scoring.requested"), any());
    }

    @Test
    void emptyPollRecordsBatchAndTransactionCostsWithoutPublishing() {
        long beforePoll = timerCount("lps.outbox.poll.cycle.duration");
        long beforeAck = timerCount("lps.outbox.kafka.ack.duration");
        long beforeConfirmation = timerCount("lps.outbox.database.confirm.duration");
        long beforeRecovery = timerCount("lps.outbox.stale.claim.scan.duration");
        long beforeClaim = timerCount("lps.outbox.claim.batch.duration");
        long beforeBatches = meters.get("lps.outbox.claim.batch.size").summary().count();
        double beforeClaimed = meters.get("lps.outbox.claim.batch.size").summary().totalAmount();

        publisher.publishPending();

        assertEquals(beforePoll + 1, timerCount("lps.outbox.poll.cycle.duration"));
        assertEquals(beforeRecovery + 1, timerCount("lps.outbox.stale.claim.scan.duration"));
        assertEquals(beforeClaim + 1, timerCount("lps.outbox.claim.batch.duration"));
        assertEquals(beforeBatches + 1, meters.get("lps.outbox.claim.batch.size").summary().count());
        assertEquals(beforeClaimed, meters.get("lps.outbox.claim.batch.size").summary().totalAmount(), 0.001);
        assertEquals(beforeAck, timerCount("lps.outbox.kafka.ack.duration"));
        assertEquals(beforeConfirmation, timerCount("lps.outbox.database.confirm.duration"));
        verifyNoInteractions(producer);
    }

    private long timerCount(String metricName) {
        return meters.get(metricName).timer().count();
    }

    @Test
    void attemptConfirmationIsIdempotentAndPreservesOriginalDeadlines() {
        service.initialize(event());
        UUID commandId = outbox.findAll().get(0).getId();
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();

        var original = attempts.findByOutboxEventId(commandId).orElseThrow();
        Instant published = original.getPublishedAt();
        Instant deadline = original.getDeadlineAt();
        Instant retryAfter = original.getRetryAfterAt();
        assertNotNull(published);
        Integer repeatedUpdate = new TransactionTemplate(transactionManager).execute(tx ->
                attempts.markPublishedIfUnpublished(commandId, published.plusSeconds(120)));
        assertNotNull(repeatedUpdate);
        assertEquals(0, repeatedUpdate.intValue());

        var persisted = attempts.findByOutboxEventId(commandId).orElseThrow();
        assertEquals(published, persisted.getPublishedAt());
        assertEquals(deadline, persisted.getDeadlineAt());
        assertEquals(retryAfter, persisted.getRetryAfterAt());
    }

    @Test
    void legacyOutboxWithoutStepAttemptStillPublishes() {
        service.initialize(event());
        UUID commandId = outbox.findAll().get(0).getId();
        jdbc.update("""
                DELETE FROM saga_orchestrator.saga_step_attempts
                WHERE outbox_event_id = ?
                """, commandId);
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPending();

        assertEquals(SagaOutboxStatus.PUBLISHED, outbox.findById(commandId).orElseThrow().getStatus());
        assertTrue(attempts.findByOutboxEventId(commandId).isEmpty());
        verify(producer, times(1)).publish(anyString(), any());
    }

    @Test
    void lostClaimOwnershipCannotActivateAttemptTimeouts() {
        service.initialize(event());
        UUID commandId = outbox.findAll().get(0).getId();
        // Simulates a different owner reclaiming the record after Kafka acknowledged it.
        when(producer.publish(anyString(), any())).thenAnswer(invocation -> {
            assertEquals(1, jdbc.update("""
                    UPDATE saga_orchestrator.saga_outbox_events
                    SET claimed_at = claimed_at + INTERVAL '1 minute'
                    WHERE id = ? AND status = 'CLAIMED'
                    """, commandId));
            return CompletableFuture.completedFuture(null);
        });

        publisher.publishPending();

        var row = outbox.findById(commandId).orElseThrow();
        assertEquals(SagaOutboxStatus.CLAIMED, row.getStatus());
        assertNull(row.getPublishedAt());
        var attempt = attempts.findByOutboxEventId(commandId).orElseThrow();
        assertNull(attempt.getPublishedAt());
        assertNull(attempt.getDeadlineAt());
        assertNull(attempt.getRetryAfterAt());
    }

    @Test
    void failedAttemptUpdateRollsBackOutboxConfirmationAndAllowsRetry() {
        service.initialize(event());
        UUID commandId = outbox.findAll().get(0).getId();
        when(producer.publish(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // NOT VALID avoids rejecting older data, but rejects any new published attempt.
        jdbc.execute("""
                ALTER TABLE saga_orchestrator.saga_step_attempts
                ADD CONSTRAINT chk_perf2b_reject_publication
                CHECK (published_at IS NULL) NOT VALID
                """);
        try {
            publisher.publishPending();
            // Kafka ack succeeded, but the confirmation transaction must have rolled back.
            var failed = outbox.findById(commandId).orElseThrow();
            assertEquals(SagaOutboxStatus.NEW, failed.getStatus());
            assertNull(failed.getPublishedAt());
            assertEquals(1, failed.getRetryCount());
            assertNull(attempts.findByOutboxEventId(commandId).orElseThrow().getPublishedAt());
        } finally {
            jdbc.execute("""
                    ALTER TABLE saga_orchestrator.saga_step_attempts
                    DROP CONSTRAINT chk_perf2b_reject_publication
                    """);
        }
        publisher.publishPending();
        assertEquals(SagaOutboxStatus.PUBLISHED, outbox.findById(commandId).orElseThrow().getStatus());
        assertNotNull(attempts.findByOutboxEventId(commandId).orElseThrow().getDeadlineAt());
        verify(producer, times(2)).publish(anyString(), any());
    }

    private ApplicationCreatedEvent event() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("10000.00"), 12, "EUR");
    }
}
