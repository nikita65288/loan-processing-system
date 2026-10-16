package com.lps.notification.outbox;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

/** Durable immutable event JSON and retry/claim bookkeeping, owned by this service. */
@Entity
@Table(name="outbox_events", schema="notification")
public class LocalOutboxEvent {
    @Id private UUID id;
    @Column(name="application_id", nullable=false) private UUID applicationId;
    @Column(nullable=false) private String topic;
    @Column(nullable=false, columnDefinition="text") private String payload;
    @Column(name="traceparent", length=55) private String traceparent;
    @Column(nullable=false) private String status;
    @Column(name="created_at", nullable=false) private Instant createdAt;
    @Column(name="claimed_at") private Instant claimedAt;
    @Column(name="published_at") private Instant publishedAt;
    @Column(name="retry_count", nullable=false) private int retryCount;
    @Column(name="last_error") private String lastError;
    protected LocalOutboxEvent() {}
    public LocalOutboxEvent(UUID id, UUID applicationId, String topic, String payload, Instant now) {
        this.id=id; this.applicationId=applicationId; this.topic=topic;
        this.payload=payload; this.traceparent=OutboxTraceContext.capture(); this.createdAt=now; this.status="NEW";
    }
    public void claim(Instant now) { status="CLAIMED"; claimedAt=now; }
    public UUID getId() { return id; }
    public UUID getApplicationId() { return applicationId; }
    public String getTopic() { return topic; }
    public String getPayload() { return payload; }
    public String getTraceparent() { return traceparent; }
    public Instant getClaimedAt() { return claimedAt; }
}
