package com.lps.sagaorchestrator.outbox;

import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.kafka.SagaCommandProducer;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
    @MockitoBean SagaCommandProducer producer;

    @BeforeEach
    void clean() {
        outbox.deleteAll();
    }

    @Test
    void publishedOnlyAfterSuccessfulKafkaAcknowledgementAndUsesApplicationKey() {
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
        ArgumentCaptor<PerformCreditScoringCommand> captor =
                ArgumentCaptor.forClass(PerformCreditScoringCommand.class);
        verify(producer).publish(eq("loan.scoring.requested"), captor.capture());
        assertEquals(saved.getId(), captor.getValue().commandId());
        assertEquals(event.applicationId(), captor.getValue().applicationId());
    }

    @Test
    void failedSendDoesNotLoseCommandAndRetryKeepsSameCommandId() {
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

    private ApplicationCreatedEvent event() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("10000.00"), 12, "EUR");
    }
}
