package com.lps.loan_application.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Getter
@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "loan_applications")
public class LoanApplication {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(
            name = "amount",
            nullable = false,
            precision = 19,
            scale = 2
    )
    private BigDecimal amount;

    @Column(name = "term", nullable = false)
    private Integer term;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private LoanApplicationStatus status;

    // HTTP idempotency metadata remains durable for the lifetime of the application.
    @Column(name = "idempotency_key", length = 128, updatable = false)
    private String idempotencyKey;

    @Column(name = "request_fingerprint", length = 64, updatable = false)
    private String requestFingerprint;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public LoanApplication(
            UUID id,
            UUID customerId,
            BigDecimal amount,
            Integer term,
            String currency
    ) {
        this.id = id;
        this.customerId = customerId;
        this.amount = amount;
        this.term = term;
        this.currency = currency;
        this.status = LoanApplicationStatus.CREATED;
    }

    /** Called before first persistence; never changes the existing creation payload. */
    public void assignIdempotency(String key, String fingerprint) {
        if (key == null || fingerprint == null || this.idempotencyKey != null) {
            throw new IllegalArgumentException("Valid idempotency metadata is required");
        }
        this.idempotencyKey = key;
        this.requestFingerprint = fingerprint;
    }

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        if (createdAt == null) {
            createdAt = now;
        }

        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    }
}