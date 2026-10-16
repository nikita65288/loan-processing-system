package com.lps.sagaorchestrator.outbox;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/** Durable physical Outbox delivery; retry records preserve the logical ID inside payload. */
@Entity
@Table(name = "saga_outbox_events", schema = "saga_orchestrator")
public class SagaOutboxEvent {
    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    @Column(name = "application_id", nullable = false, updatable = false)
    private UUID applicationId;

    @Column(name = "command_type", nullable = false, length = 128)
    private String commandType;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "traceparent", length = 55)
    private String traceparent;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private SagaOutboxStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "claimed_at")
    private Instant claimedAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    protected SagaOutboxEvent() {}

    public SagaOutboxEvent(UUID id, UUID sagaId, UUID applicationId, String commandType,
                           String topic, String payload, Instant createdAt) {
        this.id = id;
        this.sagaId = sagaId;
        this.applicationId = applicationId;
        this.commandType = commandType;
        this.topic = topic;
        this.payload = payload;
        this.traceparent = OutboxTraceContext.capture();
        this.createdAt = createdAt;
        this.status = SagaOutboxStatus.NEW;
        this.retryCount = 0;
    }

    /** Only a locked NEW record is claimed for publication. */
    public void markClaimed(Instant now) {
        this.status = SagaOutboxStatus.CLAIMED;
        this.claimedAt = now;
    }

    public UUID getId() { return id; }
    public UUID getSagaId() { return sagaId; }
    public UUID getApplicationId() { return applicationId; }
    public String getCommandType() { return commandType; }
    public String getTopic() { return topic; }
    public String getPayload() { return payload; }
    public String getTraceparent() { return traceparent; }
    public Instant getCreatedAt() { return createdAt; }
    public SagaOutboxStatus getStatus() { return status; }
    public int getRetryCount() { return retryCount; }
    public Instant getClaimedAt() { return claimedAt; }
    public Instant getPublishedAt() { return publishedAt; }
    public String getLastError() { return lastError; }
}
