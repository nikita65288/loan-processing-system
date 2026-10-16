package com.lps.loan_application.outbox;

import io.opentelemetry.context.Scope;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.loan_application.kafka.KafkaEventProducer;
import com.lps.loan_application.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reliably publishes durable Outbox records to Kafka.
 *
 * Claiming is performed in a short PostgreSQL transaction. Kafka network calls
 * happen after that transaction commits, so database locks are never held
 * while waiting for Kafka.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaEventProducer kafkaEventProducer;
    private final ObjectMapper objectMapper;
    private final int batchSize;
    private final int maxRetries;
    private final long claimTimeoutSeconds;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry metrics;
    private final Duration kafkaPublishTimeout;

    private static final long CLAIM_SAFETY_MARGIN_SECONDS = 60;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaEventProducer kafkaEventProducer,
            ObjectMapper objectMapper,
            @Value("${lps.outbox.batch-size:100}") int batchSize,
            @Value("${lps.outbox.max-retries:10}") int maxRetries,
            @Value("${lps.outbox.claim-timeout-seconds:60}") long configuredClaimTimeoutSeconds,
            @Value("${lps.outbox.kafka-publish-timeout-seconds:30}") long kafkaPublishTimeoutSeconds,
            org.springframework.transaction.PlatformTransactionManager transactionManager,
            MeterRegistry metrics
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaEventProducer = kafkaEventProducer;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;

        this.kafkaPublishTimeout = Duration.ofSeconds(kafkaPublishTimeoutSeconds);
        long derivedMin = (long) batchSize * kafkaPublishTimeoutSeconds + CLAIM_SAFETY_MARGIN_SECONDS;
        this.claimTimeoutSeconds = Math.max(configuredClaimTimeoutSeconds, derivedMin);

        if (configuredClaimTimeoutSeconds > 0 && configuredClaimTimeoutSeconds < derivedMin) {
            log.warn(
                    "Configured claim-timeout={}s is below the safe minimum {}s for batch-size={} " +
                            "and kafka-timeout={}s; using {}s instead",
                    configuredClaimTimeoutSeconds,
                    derivedMin,
                    batchSize,
                    kafkaPublishTimeoutSeconds,
                    this.claimTimeoutSeconds
            );
        }

        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${lps.outbox.polling-interval-ms:1000}")
    public void publishPendingEvents() {
        recoverStaleClaims();
        List<OutboxEvent> events = claimBatch();
        events.forEach(this::publish);
    }

    /** Recovers rows left in CLAIMED after a publisher crash. */
    private void recoverStaleClaims() {
        Instant threshold = Instant.now().minusSeconds(claimTimeoutSeconds);

        Integer recovered = transactionTemplate.execute(status ->
                outboxEventRepository.releaseStaleClaims(
                        OutboxEventStatus.CLAIMED,
                        OutboxEventStatus.NEW,
                        threshold
                )
        );

        if (recovered != null && recovered > 0) {
            log.warn("Recovered stale Outbox claims count={}", recovered);
            try { metrics.counter("lps.outbox.stale.claim.recovered").increment(recovered); } catch (RuntimeException ignored) { /* Telemetry is best effort. */ }
        }
    }

    /**
     * Locks and claims only a bounded number of NEW rows. The transaction is
     * committed before any Kafka network call starts.
     */
    private List<OutboxEvent> claimBatch() {
        List<OutboxEvent> events = transactionTemplate.execute(status -> {
            List<OutboxEvent> claimedEvents =
                    outboxEventRepository.findEventsForUpdateSkipLocked(batchSize, OutboxEventStatus.NEW.name());

            Instant claimedAt = Instant.now();
            claimedEvents.forEach(event -> event.markClaimed(claimedAt));
            outboxEventRepository.saveAll(claimedEvents);

            return claimedEvents;
        });

        List<OutboxEvent> safeEvents = events == null ? List.of() : events;

        if (!safeEvents.isEmpty()) {
            log.info("Claimed Outbox batch size={}", safeEvents.size());
        }

        return safeEvents;
    }

    private void publish(OutboxEvent outboxEvent) {
        Timer.Sample timer = Timer.start(metrics);
        CompletableFuture<SendResult<String, ApplicationCreatedEvent>> future = null;
        try (Scope ignored = OutboxTraceContext.resume(outboxEvent.getTraceparent())) {
            ApplicationCreatedEvent event =
                    objectMapper.readValue(
                            outboxEvent.getPayload(),
                            ApplicationCreatedEvent.class
                    );

            log.info(
                    "Outbox event publishing started eventId={}, aggregateId={}, eventType={}",
                    outboxEvent.getId(),
                    outboxEvent.getAggregateId(),
                    outboxEvent.getEventType()
            );

            future = kafkaEventProducer.publish(event);
            future.get(kafkaPublishTimeout.toMillis(), TimeUnit.MILLISECONDS);

            markAsPublished(outboxEvent);

            log.info(
                    "Outbox event published eventId={}, aggregateId={}, eventType={}",
                    outboxEvent.getId(),
                    outboxEvent.getAggregateId(),
                    outboxEvent.getEventType()
            );
        } catch (InterruptedException exception) {
            // Interruption usually means shutdown/cancellation, not a Kafka publication failure.
            // Keep the interrupt flag and let stale-claim recovery handle the database claim.
            Thread.currentThread().interrupt();
            cancelQuietly(future);
            log.warn(
                    "Outbox publication interrupted eventId={}, aggregateId={}",
                    outboxEvent.getId(),
                    outboxEvent.getAggregateId(),
                    exception
            );
            throw new RuntimeException("Interrupted during outbox publishing", exception);
        } catch (TimeoutException exception) {
            cancelQuietly(future);
            handlePublicationFailure(
                    outboxEvent,
                    "Kafka publish timed out after " + kafkaPublishTimeout.toMillis()
            );
        } catch (ExecutionException exception) {
            handlePublicationFailure(
                    outboxEvent,
                    conciseError(exception.getCause() == null ? exception : exception.getCause())
            );
        } catch (Exception exception) {
            handlePublicationFailure(outboxEvent, conciseError(exception));
        } finally {
            try { timer.stop(metrics.timer("lps.outbox.publication.duration")); }
            catch (RuntimeException ignored) { /* Telemetry is best effort. */ }
        }
    }

    /** Marks an event PUBLISHED only if it is still owned by this claim. */
    private void markAsPublished(OutboxEvent outboxEvent) {
        Integer updated = transactionTemplate.execute(status ->
                outboxEventRepository.markPublishedIfClaimed(
                        outboxEvent.getId(),
                        OutboxEventStatus.PUBLISHED,
                        OutboxEventStatus.CLAIMED,
                        outboxEvent.getClaimedAt(),
                        Instant.now()
                )
        );

        if (updated != null && updated == 1) {
            try { metrics.counter("lps.outbox.published").increment(); } catch (RuntimeException ignored) { /* Telemetry is best effort. */ }
            return;
        }

        log.warn(
                "Outbox event was no longer claimed when Kafka publication succeeded eventId={}",
                outboxEvent.getId()
        );
    }

    /**
     * Records the failed attempt and releases the claim unless the retry limit
     * has been reached.
     */
    private void handlePublicationFailure(OutboxEvent outboxEvent, String errorMessage) {
        Integer updated = transactionTemplate.execute(status ->
                outboxEventRepository.recordPublicationFailure(
                        outboxEvent.getId(),
                        errorMessage,
                        maxRetries,
                        OutboxEventStatus.NEW,
                        OutboxEventStatus.FAILED,
                        OutboxEventStatus.CLAIMED,
                        outboxEvent.getClaimedAt()
                )
        );

        if (updated == null || updated == 0) {
            log.warn(
                    "Outbox event failure result ignored because claim ownership changed eventId={}",
                    outboxEvent.getId()
            );
            return;
        }
        try { metrics.counter("lps.outbox.publication.failed").increment(); } catch (RuntimeException ignored) { /* Telemetry is best effort. */ }

        int nextRetryCount = outboxEvent.getRetryCount() + 1;

        if (nextRetryCount >= maxRetries) {
            log.error(
                    "Outbox event marked FAILED eventId={}, retryCount={}, error={}",
                    outboxEvent.getId(), nextRetryCount, errorMessage
            );
        } else {
            log.warn(
                    "Outbox event publication failed eventId={}, retryCount={}, error={}",
                    outboxEvent.getId(), nextRetryCount, errorMessage
            );
        }
    }

    private String conciseError(Throwable throwable) {
        String message = throwable.getMessage();

        if (message == null || message.isBlank()) {
            message = throwable.getClass().getSimpleName();
        }

        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    private void cancelQuietly(
            CompletableFuture<SendResult<String, ApplicationCreatedEvent>> future
    ) {
        if (future != null) {
            future.cancel(true);
        }
    }
}
