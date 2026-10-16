package com.lps.notification.outbox;

import io.opentelemetry.context.Scope;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** At-least-once publisher: broker ack then DB CAS; a crash may publish duplicates. */
@Component
@ConditionalOnProperty(name="lps.scheduling.enabled",havingValue="true",matchIfMissing=true)
public class LocalOutboxPublisher {
    private final LocalOutboxRepository repository;
    private final KafkaTemplate<String,String> kafka;
    private final TransactionTemplate transactions;
    private final int batchSize;
    private final int maxRetries;
    private final long claimTimeoutSeconds;
    private final MeterRegistry metrics;
    public LocalOutboxPublisher(LocalOutboxRepository repository, KafkaTemplate<String,String> kafka,
            PlatformTransactionManager manager,
            @Value("${lps.outbox.batch-size:20}") int batchSize,
            @Value("${lps.outbox.max-retries:10}") int maxRetries,
            @Value("${lps.outbox.claim-timeout-seconds:600}") long claimTimeoutSeconds,
            MeterRegistry metrics
    ) {
        this.repository=repository;
        this.kafka = kafka;
        this.metrics = metrics;
        this.transactions = new TransactionTemplate(manager);
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.claimTimeoutSeconds=Math.max(claimTimeoutSeconds, batchSize * 30L + 60);
    }
    @Scheduled(fixedDelayString="${lps.outbox.polling-interval-ms:1000}")
    public void publishPending() {
        Integer recovered = transactions.execute(x -> repository.releaseStale(
                Instant.now().minusSeconds(claimTimeoutSeconds)));
        if (recovered != null && recovered > 0)
            try { metrics.counter(
                    "lps.outbox.stale.claim.recovered").increment(recovered);
            } catch (RuntimeException ignored) {
                /* Telemetry is best effort. */
            }
        List<LocalOutboxEvent> claimed=transactions.execute(x -> {
            var rows=repository.claimable(batchSize);
            Instant now=Instant.now();
            rows.forEach(e -> e.claim(now));
            repository.saveAll(rows);
            return rows;
        });
        if (claimed == null) return;
        for (LocalOutboxEvent event : claimed) publishOne(event);
    }
    private void publishOne(LocalOutboxEvent event) {
        Timer.Sample timer = Timer.start(metrics);
        try (Scope ignored = OutboxTraceContext.resume(event.getTraceparent())) {
            // Payload is serialized once when the business transaction commits.
            kafka.send(event.getTopic(),event.getApplicationId().toString(),event.getPayload())
                    .get(30,TimeUnit.SECONDS);
            Integer updated = transactions.execute(x -> repository.publishIfOwned(
                    event.getId(),event.getClaimedAt(),Instant.now()));
            if (updated != null && updated == 1) try {
                metrics.counter("lps.outbox.published").increment();
            } catch (RuntimeException ignoredRuntimeException) {
                /* Telemetry is best effort. */
            }
        } catch (Exception ex) {
            String detail=ex.getClass().getSimpleName();
            Integer updated = transactions.execute(x -> repository.failIfOwned(
                    event.getId(),event.getClaimedAt(),detail,maxRetries));
            if (updated != null && updated == 1)
                try { metrics.counter(
                        "lps.outbox.publication.failed").increment();
                } catch (RuntimeException ignored) {
                    /* Telemetry is best effort. */
                }
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
        } finally {
            try {
                timer.stop(metrics.timer("lps.outbox.publication.duration"));
            }
            catch (RuntimeException ignored) {
                /* Telemetry is best effort. */
            }
        }
    }
}
