package com.lps.loan_application.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.loan_application.kafka.KafkaEventProducer;
import com.lps.loan_application.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {

    private OutboxEventRepository repository;
    private KafkaEventProducer kafkaEventProducer;
    private PlatformTransactionManager transactionManager;
    private TransactionStatus transactionStatus;
    private OutboxPublisher publisher;

    private static final int BATCH_SIZE = 30;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafkaEventProducer = mock(KafkaEventProducer.class);
        transactionManager = mock(PlatformTransactionManager.class);
        transactionStatus = mock(TransactionStatus.class);

        when(transactionManager.getTransaction(any())).thenReturn(transactionStatus);

        publisher = new OutboxPublisher(
                repository,
                kafkaEventProducer,
                new ObjectMapper().registerModule(new JavaTimeModule()),
                BATCH_SIZE,
                3,
                60,
                30,
                transactionManager,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        );
    }

    @Test
    void shouldClaimAndPublishEvent() {
        ApplicationCreatedEvent event = createEvent();
        OutboxEvent outboxEvent = createOutboxEvent(event);

        when(repository.findEventsForUpdateSkipLocked(BATCH_SIZE, OutboxEventStatus.NEW.name()))
                .thenReturn(List.of(outboxEvent));
        when(kafkaEventProducer.publish(event))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        when(repository.markPublishedIfClaimed(
                eq(outboxEvent.getId()),
                eq(OutboxEventStatus.PUBLISHED),
                eq(OutboxEventStatus.CLAIMED),
                any(Instant.class),
                any(Instant.class)
        )).thenAnswer(invocation -> {
            outboxEvent.markPublished(invocation.getArgument(4));
            return 1;
        });

        publisher.publishPendingEvents();

        assertEquals(OutboxEventStatus.PUBLISHED, outboxEvent.getStatus());
        verify(repository).saveAll(List.of(outboxEvent));
        verify(repository).markPublishedIfClaimed(
                eq(outboxEvent.getId()),
                eq(OutboxEventStatus.PUBLISHED),
                eq(OutboxEventStatus.CLAIMED),
                any(Instant.class),
                any(Instant.class)
        );
        verify(transactionManager, atLeast(3)).commit(transactionStatus);
    }

    @Test
    void shouldIncrementRetryAndReturnEventToNewAfterKafkaFailure() {
        ApplicationCreatedEvent event = createEvent();
        OutboxEvent outboxEvent = createOutboxEvent(event);

        when(repository.findEventsForUpdateSkipLocked(BATCH_SIZE, OutboxEventStatus.NEW.name()))
                .thenReturn(List.of(outboxEvent));

        CompletableFuture<SendResult<String, ApplicationCreatedEvent>> failedFuture =
                new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException("Kafka unavailable"));

        when(kafkaEventProducer.publish(event)).thenReturn(failedFuture);
        when(repository.recordPublicationFailure(
                eq(outboxEvent.getId()),
                contains("Kafka unavailable"),
                eq(3),
                eq(OutboxEventStatus.NEW),
                eq(OutboxEventStatus.FAILED),
                eq(OutboxEventStatus.CLAIMED),
                any(Instant.class)
        )).thenReturn(1);

        publisher.publishPendingEvents();

        verify(repository).recordPublicationFailure(
                eq(outboxEvent.getId()),
                contains("Kafka unavailable"),
                eq(3),
                eq(OutboxEventStatus.NEW),
                eq(OutboxEventStatus.FAILED),
                eq(OutboxEventStatus.CLAIMED),
                any(Instant.class)
        );
    }

    @Test
    void shouldMarkEventFailedWhenRetryLimitIsReached() {
        ApplicationCreatedEvent event = createEvent();
        OutboxEvent outboxEvent = createOutboxEvent(event);

        // The entity already failed twice; this attempt is the third.
        outboxEvent.markPublicationFailed("first", 3);
        outboxEvent.markClaimed(Instant.now());

        when(repository.findEventsForUpdateSkipLocked(BATCH_SIZE, OutboxEventStatus.NEW.name()))
                .thenReturn(List.of(outboxEvent));

        CompletableFuture<SendResult<String, ApplicationCreatedEvent>> failedFuture =
                new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException("permanent failure"));

        when(kafkaEventProducer.publish(event)).thenReturn(failedFuture);
        when(repository.recordPublicationFailure(
                eq(outboxEvent.getId()),
                contains("permanent failure"),
                eq(3),
                eq(OutboxEventStatus.NEW),
                eq(OutboxEventStatus.FAILED),
                eq(OutboxEventStatus.CLAIMED),
                any(Instant.class)
        )).thenReturn(1);

        publisher.publishPendingEvents();

        verify(repository).recordPublicationFailure(
                eq(outboxEvent.getId()),
                contains("permanent failure"),
                eq(3),
                eq(OutboxEventStatus.NEW),
                eq(OutboxEventStatus.FAILED),
                eq(OutboxEventStatus.CLAIMED),
                any(Instant.class)
        );
    }


    @Test
    void shouldPropagateInterruptionWithoutRecordingKafkaFailure() throws Exception {
        ApplicationCreatedEvent event = createEvent();
        OutboxEvent outboxEvent = createOutboxEvent(event);

        when(repository.findEventsForUpdateSkipLocked(BATCH_SIZE, OutboxEventStatus.NEW.name()))
                .thenReturn(List.of(outboxEvent));

        CompletableFuture<SendResult<String, ApplicationCreatedEvent>> future =
                new CompletableFuture<>();
        when(kafkaEventProducer.publish(event)).thenReturn(future);

        Thread.currentThread().interrupt();
        try {
            assertThrows(RuntimeException.class, publisher::publishPendingEvents);
            assertTrue(Thread.currentThread().isInterrupted());
            verify(repository, never()).recordPublicationFailure(
                    any(), anyString(), anyInt(), any(), any(), any(), any()
            );
            assertTrue(future.isCancelled() || future.isCompletedExceptionally());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void shouldRecoverStaleClaimsBeforeClaimingNewEvents() {
        when(repository.releaseStaleClaims(
                eq(OutboxEventStatus.CLAIMED),
                eq(OutboxEventStatus.NEW),
                any(Instant.class)
        )).thenReturn(2);

        when(repository.findEventsForUpdateSkipLocked(BATCH_SIZE, OutboxEventStatus.NEW.name()))
                .thenReturn(List.of());

        publisher.publishPendingEvents();

        verify(repository).releaseStaleClaims(
                eq(OutboxEventStatus.CLAIMED),
                eq(OutboxEventStatus.NEW),
                any(Instant.class)
        );
        verify(repository).findEventsForUpdateSkipLocked(BATCH_SIZE, OutboxEventStatus.NEW.name());
    }

    private ApplicationCreatedEvent createEvent() {
        return new ApplicationCreatedEvent(
                UUID.randomUUID(),
                "ApplicationCreated",
                Instant.now(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("10000.00"),
                12,
                "EUR"
        );
    }

    private OutboxEvent createOutboxEvent(ApplicationCreatedEvent event) {
        try {
            return new OutboxEvent(
                    event.eventId(),
                    "LOAN_APPLICATION",
                    event.applicationId(),
                    "LoanApplicationCreated",
                    new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(event),
                    Instant.now()
            );
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
