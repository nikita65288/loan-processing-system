package com.lps.sagaorchestrator.retry;

import com.lps.sagaorchestrator.saga.SagaStep;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Type-safe timeout policy; every new step snapshots durations into PostgreSQL. */
@ConfigurationProperties(prefix = "lps.saga")
public record SagaRetryProperties(Timeout timeout, Retry retry, Scheduler scheduler) {
    public SagaRetryProperties {
        if (timeout == null || retry == null || scheduler == null) {
            throw new IllegalArgumentException("Saga timeout, retry and scheduler configuration are required");
        }
    }

    public record Timeout(boolean enabled, Duration scoring, Duration bureau, Duration decision,
                          Duration disbursement, Duration notification) {
        // The overload preserves compatibility with the original three-step constructor.
        public Timeout(boolean enabled, Duration scoring, Duration bureau, Duration decision) {
            this(enabled,scoring,bureau,decision,Duration.ofSeconds(60),Duration.ofSeconds(30));
        }
        @org.springframework.boot.context.properties.bind.ConstructorBinding
        public Timeout {
            if (scoring == null || scoring.isNegative() || scoring.isZero()
                    || bureau == null || bureau.isNegative() || bureau.isZero()
                    || decision == null || decision.isNegative() || decision.isZero()
                    || scoring.toSeconds() < 1 || bureau.toSeconds() < 1 || decision.toSeconds() < 1
                    || scoring.getNano() != 0 || bureau.getNano() != 0 || decision.getNano() != 0
                    || disbursement == null || notification == null || disbursement.isNegative()
                    || notification.isNegative() || disbursement.toSeconds() < 1
                    || notification.toSeconds() < 1 || disbursement.getNano()!=0 || notification.getNano()!=0) {
                throw new IllegalArgumentException("Saga timeouts must be whole positive seconds");
            }
        }
    }

    public record Retry(int maxAttempts, Duration backoff) {
        public Retry {
            if (maxAttempts < 1 || backoff == null || backoff.isNegative()
                    || backoff.getNano() != 0) {
                throw new IllegalArgumentException("Saga retry maxAttempts must be positive and backoff whole seconds");
            }
        }
    }

    public record Scheduler(int batchSize) {
        public Scheduler {
            if (batchSize < 1) throw new IllegalArgumentException("Saga scheduler batchSize must be positive");
        }
    }

    public Duration timeoutFor(SagaStep step) {
        return switch (step) {
            case SCORING -> timeout.scoring();
            case BUREAU -> timeout.bureau();
            case DECISION -> timeout.decision();
            case DISBURSEMENT -> timeout.disbursement();
            case NOTIFICATION -> timeout.notification();
            case FINISHED -> throw new IllegalArgumentException("FINISHED is not a retryable step");
        };
    }
}
