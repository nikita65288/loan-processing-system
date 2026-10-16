package com.lps.sagaorchestrator.inbox;

import org.springframework.jdbc.core.JdbcTemplate;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.stereotype.Repository;

import java.util.Objects;
import java.util.UUID;

/** Registers incoming Kafka events within the caller's Saga/JPA transaction. */
@Repository
public class ProcessedMessageDao {
    private static final String CONSUMER = "saga-orchestrator-service";
    private final JdbcTemplate jdbc;
    private final MeterRegistry metrics;

    public ProcessedMessageDao(JdbcTemplate jdbc, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.metrics = metrics;
    }

    /** The PostgreSQL primary key arbitrates concurrent delivery without a SELECT/INSERT race. */
    public boolean register(UUID eventId, String eventType) {
        Objects.requireNonNull(eventId, "eventId is required");
        Objects.requireNonNull(eventType, "eventType is required");
        boolean inserted = jdbc.update("""
                INSERT INTO saga_orchestrator.processed_messages
                    (consumer_name, message_id, message_type, processed_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (consumer_name, message_id) DO NOTHING
                """, CONSUMER, eventId, eventType) == 1;
        String outcome = inserted ? "new" : "duplicate";
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() {
                    try { metrics.counter("lps.inbox.messages", "outcome", outcome).increment(); }
                    catch (RuntimeException ignored) { /* Metrics cannot break committed processing. */ }
                }
            });
        }
        return inserted;
    }

    /** Serializes creation for the same application before a Saga row exists to lock. */
    public void lockApplicationInitialization(UUID applicationId) {
        Objects.requireNonNull(applicationId, "applicationId is required");
        // Transaction-scoped advisory lock; hash collisions only reduce concurrency.
        jdbc.queryForList("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", applicationId.toString());
    }
}
