package com.lps.sagaorchestrator.outbox;

import io.opentelemetry.context.Scope;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.contracts.command.SendLoanDecisionNotificationCommand;
import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.sagaorchestrator.kafka.SagaKafkaConfiguration;
import com.lps.sagaorchestrator.kafka.SagaCommandProducer;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Uses the established claim/publish/confirm pattern without holding DB locks during Kafka IO. */
@Component
public class SagaOutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(SagaOutboxPublisher.class);
    private final SagaOutboxRepository repository;
    private final SagaStepAttemptRepository attempts;
    private final Clock clock;
    private final SagaCommandProducer producer;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final int maxRetries;
    private final long claimTimeoutSeconds;
    private final long publishTimeoutSeconds;
    private final MeterRegistry metrics;

    public SagaOutboxPublisher(
            SagaOutboxRepository repository,
            SagaStepAttemptRepository attempts,
            Clock clock,
            SagaCommandProducer producer,
            ObjectMapper mapper,
            PlatformTransactionManager transactionManager,
            @Value("${lps.outbox.batch-size:20}") int batchSize,
            @Value("${lps.outbox.max-retries:10}") int maxRetries,
            @Value("${lps.outbox.claim-timeout-seconds:60}") long claimTimeoutSeconds,
            @Value("${lps.outbox.kafka-publish-timeout-seconds:30}") long publishTimeoutSeconds,
            MeterRegistry metrics
    ) {
        if (batchSize < 1 || maxRetries < 1 || publishTimeoutSeconds < 1) {
            throw new IllegalArgumentException("Outbox batch size, retries and publish timeout must be positive");
        }
        this.repository = repository;
        this.metrics = metrics;
        this.attempts = attempts;
        this.clock = clock;
        this.producer = producer;
        this.mapper = mapper;
        this.tx = new TransactionTemplate(transactionManager);
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.publishTimeoutSeconds = publishTimeoutSeconds;
        // A claim cannot expire during a sequential full batch even on slow Kafka acknowledgments.
        this.claimTimeoutSeconds = Math.max(claimTimeoutSeconds, batchSize * publishTimeoutSeconds + 60L);
    }

    @Scheduled(fixedDelayString = "${lps.outbox.polling-interval-ms:1000}")
    public void publishPending() {
        recoverStaleClaims();
        claimBatch().forEach(this::publishOne);
    }

    private void recoverStaleClaims() {
        Instant threshold = Instant.now().minusSeconds(claimTimeoutSeconds);
        Integer n = tx.execute(ignored -> repository.releaseStaleClaims(
                SagaOutboxStatus.CLAIMED, SagaOutboxStatus.NEW, threshold));
        if (n != null && n > 0) {
            log.warn("Recovered {} abandoned Saga Outbox claims", n);
            try { metrics.counter("lps.outbox.stale.claim.recovered").increment(n); } catch (RuntimeException ignored) { /* Telemetry is best effort. */ }
        }
    }

    /** Claim in a short PostgreSQL transaction; another instance skips locked rows. */
    private List<SagaOutboxEvent> claimBatch() {
        List<SagaOutboxEvent> events = tx.execute(ignored -> {
            List<SagaOutboxEvent> records = repository.claimable(batchSize);
            Instant now = Instant.now();
            records.forEach(record -> record.markClaimed(now));
            repository.saveAll(records);
            return records;
        });
        return events == null ? List.of() : events;
    }

    private void publishOne(SagaOutboxEvent record) {
        Timer.Sample timer = Timer.start(metrics);
        CompletableFuture<?> future = null;
        try (Scope ignored = OutboxTraceContext.resume(record.getTraceparent())) {
            // Restrict deserialization to known persisted command types and their expected topics.
            Object command = switch (record.getCommandType()) {
                case "PerformCreditScoring" -> {
                    requireTopic(record, SagaKafkaConfiguration.SCORING_REQUESTED_TOPIC);
                    yield mapper.readValue(record.getPayload(), PerformCreditScoringCommand.class);
                }
                case "PerformBureauCheck" -> {
                    requireTopic(record, SagaKafkaConfiguration.BUREAU_REQUESTED_TOPIC);
                    yield mapper.readValue(record.getPayload(), PerformBureauCheckCommand.class);
                }
                case "MakeDecision" -> {
                    requireTopic(record, SagaKafkaConfiguration.DECISION_REQUESTED_TOPIC);
                    yield mapper.readValue(record.getPayload(), MakeDecisionCommand.class);
                }
                case "DisburseLoan" -> {
                    requireTopic(record,SagaKafkaConfiguration.DISBURSEMENT_REQUESTED_TOPIC);
                    yield mapper.readValue(record.getPayload(),DisburseLoanCommand.class);
                }
                case "SendLoanDecisionNotification" -> {
                    requireTopic(record,SagaKafkaConfiguration.NOTIFICATION_REQUESTED_TOPIC);
                    yield mapper.readValue(record.getPayload(),SendLoanDecisionNotificationCommand.class);
                }
                default -> throw new IllegalArgumentException("Unexpected Saga command type: " + record.getCommandType());
            };
            future = producer.publish(record.getTopic(), command);
            future.get(publishTimeoutSeconds, TimeUnit.SECONDS); // broker acknowledgement
            Integer updated = tx.execute(x -> {
                Instant confirmedAt = clock.instant();
                int rows = repository.markPublishedIfOwned(
                        record.getId(), SagaOutboxStatus.PUBLISHED, SagaOutboxStatus.CLAIMED,
                        record.getClaimedAt(), confirmedAt);
                if (rows == 1) {
                    // Older Outbox records may have no attempt metadata.
                    attempts.findByOutboxEventId(record.getId())
                            .ifPresent(attempt -> attempt.markPublished(confirmedAt));
                }
                return rows;
            });
            if (updated == null || updated != 1) {
                log.warn("Kafka accepted command but claim ownership changed commandId={}", record.getId());
            } else {
                try {
                    metrics.counter("lps.outbox.published").increment();
                } catch (RuntimeException ignoredRuntimeException) {
                    /* Telemetry is best effort. */
                }
                log.info("Published Saga Outbox record outboxId={}, applicationId={}",
                        record.getId(), record.getApplicationId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (future != null) future.cancel(false);
            throw new IllegalStateException("Interrupted while publishing Saga command", e);
        } catch (TimeoutException e) {
            if (future != null) future.cancel(false);
            fail(record, "Kafka acknowledgement timeout");
        } catch (ExecutionException e) {
            fail(record, message(e.getCause() == null ? e : e.getCause()));
        } catch (Exception e) {
            fail(record, message(e));
        } finally {
            try { timer.stop(metrics.timer("lps.outbox.publication.duration")); }
            catch (RuntimeException ignored) { /* Telemetry is best effort. */ }
        }
    }

    private void requireTopic(SagaOutboxEvent record, String expectedTopic) {
        if (!expectedTopic.equals(record.getTopic())) {
            throw new IllegalStateException("Invalid topic for " + record.getCommandType());
        }
    }

    private void fail(SagaOutboxEvent record, String error) {
        Integer updated = tx.execute(ignored -> repository.recordFailureIfOwned(
                record.getId(), error, maxRetries, SagaOutboxStatus.NEW, SagaOutboxStatus.FAILED,
                SagaOutboxStatus.CLAIMED, record.getClaimedAt()));
        if (updated != null && updated == 1) {
            try { metrics.counter("lps.outbox.publication.failed").increment(); } catch (RuntimeException ignored) { /* Telemetry is best effort. */ }
            log.warn("Saga Outbox publish failed commandId={}, retry={}, maxRetries={}, cause={}",
                    record.getId(), record.getRetryCount() + 1, maxRetries, error);
        }
    }

    private String message(Throwable e) {
        String detail = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return detail.substring(0, Math.min(detail.length(), 1000));
    }
}
