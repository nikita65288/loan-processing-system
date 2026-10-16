package com.lps.sagaorchestrator.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

/** Runs bounded due-only scans; RetryService owns every transactional state change. */
@Component
@ConditionalOnProperty(name = "lps.saga.timeout.enabled", havingValue = "true", matchIfMissing = true)
public class SagaTimeoutScheduler {
    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutScheduler.class);
    private final SagaStepAttemptRepository attempts;
    private final SagaRetryService retry;
    private final SagaRetryProperties policy;
    private final Clock clock;
    // Rotating scan offset is a fairness hint only; all real deadlines live in PostgreSQL.
    private final AtomicInteger offset = new AtomicInteger();

    public SagaTimeoutScheduler(SagaStepAttemptRepository attempts, SagaRetryService retry,
                                SagaRetryProperties policy, Clock clock) {
        this.attempts = attempts;
        this.retry = retry;
        this.policy = policy;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${lps.saga.scheduler.interval-ms:1000}")
    public void checkTimeouts() {
        int batchSize = policy.scheduler().batchSize();
        int scanOffset = offset.get();
        var due = attempts.findDueSagaIds(clock.instant(), policy.retry().maxAttempts(),
                batchSize, scanOffset);
        // Rotate past repeatedly failing candidates so a full poison batch cannot starve later Saga.
        if (due.size() < batchSize || scanOffset > Integer.MAX_VALUE - batchSize) {
            offset.set(0);
        } else {
            offset.set(scanOffset + batchSize);
        }
        for (var sagaId : due) {
            try {
                retry.process(sagaId);
            } catch (Exception exception) {
                // Do not prevent other Saga from progressing; next poll retries this candidate.
                log.error("Saga timeout processing failed sagaId={}", sagaId, exception);
            }
        }
    }
}
