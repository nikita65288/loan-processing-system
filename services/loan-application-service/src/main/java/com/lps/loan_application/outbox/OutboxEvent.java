package com.lps.loan_application.outbox;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

/**
 * Durable record of an event that must be published to Kafka.
 *
 * The record is created in the same PostgreSQL transaction as the business
 * entity. Publisher state is then managed separately so Kafka failures
 * do not roll back the business transaction.
 */
@Entity
@Getter
@Table(
        name = "outbox_events",
        indexes = {
                @Index(name = "idx_outbox_events_status_created_at", columnList = "status, created_at"),
                @Index(name = "idx_outbox_events_status_claimed_at", columnList = "status, claimed_at"),
                @Index(name = "idx_outbox_events_status_published_at", columnList = "status, published_at")
        }
)
public class OutboxEvent {
    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 64)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false, length = 128)
    private String eventType;

    @Column(name = "traceparent", length = 55)
    private String traceparent;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private OutboxEventStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "claimed_at")
    private Instant claimedAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {}

    public OutboxEvent(UUID id, String aggregateType, UUID aggregateId, String eventType,
                       String payload, Instant createdAt) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.traceparent = OutboxTraceContext.capture();
        this.createdAt = createdAt;
        this.status = OutboxEventStatus.NEW;
        this.retryCount = 0;
    }

    /** Claims the event after the row has been locked in PostgreSQL. */
    public void markClaimed(Instant claimedAt) {
        this.status = OutboxEventStatus.CLAIMED;
        this.claimedAt = claimedAt;
    }

    /** Marks the event as published after Kafka confirms successful publication. */
    public void markPublished(Instant publishedAt) {
        this.status = OutboxEventStatus.PUBLISHED;
        this.claimedAt = null;
        this.publishedAt = publishedAt;
        this.lastError = null;
    }

    /** Records a failed publication and decides whether to retry or fail permanently. */
    public void markPublicationFailed(String errorMessage, int maxRetries) {
        retryCount++;
        lastError = errorMessage;
        claimedAt = null;
        status = retryCount >= maxRetries ? OutboxEventStatus.FAILED : OutboxEventStatus.NEW;
    }

    /** Releases an abandoned claim so another publisher can process the event. */
    public void releaseClaim() {
        status = OutboxEventStatus.NEW;
        claimedAt = null;
    }
}
