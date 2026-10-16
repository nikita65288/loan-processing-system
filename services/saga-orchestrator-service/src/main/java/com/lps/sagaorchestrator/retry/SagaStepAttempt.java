package com.lps.sagaorchestrator.retry;

import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaStep;
import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/** Durable delivery attempts of one logical command; payload/identity never change across retries. */
@Entity
@Table(name = "saga_step_attempts", schema = "saga_orchestrator")
public class SagaStepAttempt {
    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    @Enumerated(EnumType.STRING)
    @Column(name = "step", nullable = false, updatable = false, length = 32)
    private SagaStep step;

    @Column(name = "attempt_number", nullable = false, updatable = false)
    private int attemptNumber;

    @Column(name = "command_id", nullable = false, updatable = false)
    private UUID commandId;

    @Column(name = "outbox_event_id", nullable = false, updatable = false, unique = true)
    private UUID outboxEventId;

    @Column(name = "command_type", nullable = false, updatable = false, length = 128)
    private String commandType;

    @Column(name = "topic", nullable = false, updatable = false, length = 128)
    private String topic;

    @Column(name = "payload", nullable = false, updatable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "timeout_seconds", nullable = false, updatable = false)
    private long timeoutSeconds;

    @Column(name = "backoff_seconds", nullable = false, updatable = false)
    private long backoffSeconds;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "deadline_at")
    private Instant deadlineAt;

    @Column(name = "retry_after_at")
    private Instant retryAfterAt;

    protected SagaStepAttempt() {}

    /** First attempt is persisted with the original Outbox record in one transaction. */
    public static SagaStepAttempt initial(SagaInstance saga, SagaStep step, UUID commandId,
                                          String commandType, String topic, String payload,
                                          Instant now, SagaRetryProperties policy) {
        SagaStepAttempt attempt = new SagaStepAttempt();
        attempt.id = UUID.randomUUID();
        attempt.sagaId = saga.getSagaId();
        attempt.step = step;
        attempt.attemptNumber = 1;
        attempt.commandId = commandId;
        attempt.outboxEventId = commandId;
        attempt.commandType = commandType;
        attempt.topic = topic;
        attempt.payload = payload;
        attempt.timeoutSeconds = policy.timeoutFor(step).toSeconds();
        attempt.backoffSeconds = policy.retry().backoff().toSeconds();
        attempt.createdAt = now;
        return attempt;
    }

    /** New delivery has a fresh Outbox id but preserves the logical command and its payload. */
    public static SagaStepAttempt retry(SagaStepAttempt previous, UUID nextOutboxId, Instant now) {
        if (previous.publishedAt == null) throw new IllegalStateException("Previous command was not published");
        SagaStepAttempt attempt = new SagaStepAttempt();
        attempt.id = UUID.randomUUID();
        attempt.sagaId = previous.sagaId;
        attempt.step = previous.step;
        attempt.attemptNumber = previous.attemptNumber + 1;
        attempt.commandId = previous.commandId;
        attempt.outboxEventId = nextOutboxId;
        attempt.commandType = previous.commandType;
        attempt.topic = previous.topic;
        attempt.payload = previous.payload;
        attempt.timeoutSeconds = previous.timeoutSeconds;
        attempt.backoffSeconds = previous.backoffSeconds;
        attempt.createdAt = now;
        return attempt;
    }

    /** Kafka acknowledgement and timeout activation commit together, after the network send. */
    public void markPublished(Instant now) {
        if (publishedAt != null) return;
        publishedAt = now;
        deadlineAt = now.plusSeconds(timeoutSeconds);
        retryAfterAt = deadlineAt.plusSeconds(backoffSeconds);
    }

    public UUID getSagaId() { return sagaId; }
    public SagaStep getStep() { return step; }
    public int getAttemptNumber() { return attemptNumber; }
    public UUID getCommandId() { return commandId; }
    public UUID getOutboxEventId() { return outboxEventId; }
    public String getCommandType() { return commandType; }
    public String getTopic() { return topic; }
    public String getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
    public Instant getDeadlineAt() { return deadlineAt; }
    public Instant getRetryAfterAt() { return retryAfterAt; }
}
