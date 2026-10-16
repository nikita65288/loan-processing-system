package com.lps.sagaorchestrator.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/** PostgreSQL claim operations are safe with multiple publisher instances. */
public interface SagaOutboxRepository extends JpaRepository<SagaOutboxEvent, UUID> {
    long countByApplicationId(UUID applicationId);

    /** Load only metadata, not potentially sensitive command JSON, for recovery inspection. */
    @Query("""
            SELECT e.applicationId AS applicationId, e.commandType AS commandType,
                   e.status AS status, e.id AS id, e.createdAt AS createdAt
            FROM SagaOutboxEvent e WHERE e.applicationId IN :applicationIds
            """)
    List<SagaOutboxDiagnosticRow> findDiagnosticRows(
            @Param("applicationIds") Collection<UUID> applicationIds);

    @Query(value = """
            SELECT * FROM saga_orchestrator.saga_outbox_events
            WHERE status = 'NEW'
            ORDER BY created_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<SagaOutboxEvent> claimable(@Param("batchSize") int batchSize);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE SagaOutboxEvent e
            SET e.status = :newStatus, e.claimedAt = null
            WHERE e.status = :claimed AND e.claimedAt < :threshold
            """)
    int releaseStaleClaims(@Param("claimed") SagaOutboxStatus claimed,
                           @Param("newStatus") SagaOutboxStatus newStatus,
                           @Param("threshold") Instant threshold);

    /** A stale/late publisher must not overwrite a newer claim. */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE SagaOutboxEvent e
            SET e.status = :published, e.claimedAt = null,
                e.publishedAt = :now, e.lastError = null
            WHERE e.id = :id AND e.status = :claimed AND e.claimedAt = :claimedAt
            """)
    int markPublishedIfOwned(@Param("id") UUID id,
                             @Param("published") SagaOutboxStatus published,
                             @Param("claimed") SagaOutboxStatus claimed,
                             @Param("claimedAt") Instant claimedAt,
                             @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE SagaOutboxEvent e
            SET e.retryCount = e.retryCount + 1,
                e.lastError = :error, e.claimedAt = null,
                e.status = CASE WHEN e.retryCount + 1 >= :maxRetries
                                THEN :failed ELSE :pending END
            WHERE e.id = :id AND e.status = :claimed AND e.claimedAt = :claimedAt
            """)
    int recordFailureIfOwned(@Param("id") UUID id,
                             @Param("error") String error,
                             @Param("maxRetries") int maxRetries,
                             @Param("pending") SagaOutboxStatus pending,
                             @Param("failed") SagaOutboxStatus failed,
                             @Param("claimed") SagaOutboxStatus claimed,
                             @Param("claimedAt") Instant claimedAt);
}
