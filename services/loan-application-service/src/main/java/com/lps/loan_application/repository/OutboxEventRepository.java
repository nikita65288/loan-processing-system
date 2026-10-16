package com.lps.loan_application.repository;

import com.lps.loan_application.outbox.OutboxEvent;
import com.lps.loan_application.outbox.OutboxEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Claims a bounded batch. PostgreSQL skips rows already locked by another
     * publisher, allowing multiple publisher instances to process different events.
     */
    @Query(
            value = "SELECT * FROM outbox_events " +
                    "WHERE status = :status " +
                    "ORDER BY created_at " +
                    "LIMIT :batchSize " +
                    "FOR UPDATE SKIP LOCKED",
            nativeQuery = true
    )
    List<OutboxEvent> findEventsForUpdateSkipLocked(
            @Param("batchSize") int batchSize,
            @Param("status") String status
    );

    /** Releases claims left behind by a crashed publisher. */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE OutboxEvent event
            SET event.status = :newStatus,
                event.claimedAt = null
            WHERE event.status = :claimedStatus
              AND event.claimedAt < :threshold
            """)
    int releaseStaleClaims(
            @Param("claimedStatus") OutboxEventStatus claimedStatus,
            @Param("newStatus") OutboxEventStatus newStatus,
            @Param("threshold") Instant threshold
    );

    /** Deletes only old successfully published records. */
    @Modifying(clearAutomatically = true)
    @Query("""
            DELETE FROM OutboxEvent event
            WHERE event.status = :status
              AND event.publishedAt < :threshold
            """)
    int deletePublishedBefore(
            @Param("status") OutboxEventStatus status,
            @Param("threshold") Instant threshold
    );

    /**
     * Updates publication state only while the event is still claimed.
     * A late publisher result therefore cannot overwrite a state changed
     * after stale-claim recovery.
     */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE OutboxEvent event
            SET event.status = :status,
                event.claimedAt = null,
                event.publishedAt = :publishedAt,
                event.lastError = null
            WHERE event.id = :id
              AND event.status = :claimedStatus
              AND event.claimedAt = :claimedAt
            """)
    int markPublishedIfClaimed(
            @Param("id") UUID id,
            @Param("status") OutboxEventStatus status,
            @Param("claimedStatus") OutboxEventStatus claimedStatus,
            @Param("claimedAt") Instant claimedAt,
            @Param("publishedAt") Instant publishedAt
    );

    /** Records a failed attempt only while this publisher still owns the claim. */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE OutboxEvent event
            SET event.retryCount = event.retryCount + 1,
                event.lastError = :lastError,
                event.claimedAt = null,
                event.status = CASE
                    WHEN event.retryCount + 1 >= :maxRetries
                    THEN :failedStatus
                    ELSE :newStatus
                END
            WHERE event.id = :id
              AND event.status = :claimedStatus
              AND event.claimedAt = :claimedAt
            """)
    int recordPublicationFailure(
            @Param("id") UUID id,
            @Param("lastError") String lastError,
            @Param("maxRetries") int maxRetries,
            @Param("newStatus") OutboxEventStatus newStatus,
            @Param("failedStatus") OutboxEventStatus failedStatus,
            @Param("claimedStatus") OutboxEventStatus claimedStatus,
            @Param("claimedAt") Instant claimedAt
    );
}
