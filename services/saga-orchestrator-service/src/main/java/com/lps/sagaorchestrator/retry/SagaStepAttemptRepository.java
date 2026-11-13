package com.lps.sagaorchestrator.retry;

import com.lps.sagaorchestrator.saga.SagaStep;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Finds only due current-step attempts; never scans every active Saga in memory. */
public interface SagaStepAttemptRepository extends JpaRepository<SagaStepAttempt, UUID> {
    Optional<SagaStepAttempt> findFirstBySagaIdAndStepOrderByAttemptNumberDesc(UUID sagaId, SagaStep step);
    Optional<SagaStepAttempt> findByOutboxEventId(UUID outboxEventId);

    /**
     * Activates the attempt deadline only after owned Outbox confirmation.
     * Preserves original timeout/backoff policy and prevents repeated updates.
     */
    @Modifying
    @Query(value = """
        UPDATE saga_orchestrator.saga_step_attempts
        SET published_at = :confirmedAt,
            deadline_at =
                CAST(:confirmedAt AS timestamp with time zone)
                + timeout_seconds * INTERVAL '1 second',
            retry_after_at =
                CAST(:confirmedAt AS timestamp with time zone)
                + timeout_seconds * INTERVAL '1 second'
                + backoff_seconds * INTERVAL '1 second'
        WHERE outbox_event_id = :outboxEventId
          AND published_at IS NULL
        """, nativeQuery = true)
    int markPublishedIfUnpublished(
            @Param("outboxEventId") UUID outboxEventId,
            @Param("confirmedAt") Instant confirmedAt
    );

    @Query(value = """
            SELECT a.saga_id
            FROM saga_orchestrator.saga_step_attempts a
            JOIN saga_orchestrator.saga_instances s ON s.saga_id = a.saga_id
            WHERE s.status = 'IN_PROGRESS'
              AND s.current_step = a.step
              AND a.published_at IS NOT NULL
              AND a.deadline_at <= :now
              AND (CASE WHEN a.attempt_number >= :maxAttempts
                        THEN a.deadline_at ELSE a.retry_after_at END) <= :now
              AND NOT EXISTS (
                  SELECT 1 FROM saga_orchestrator.saga_step_attempts next_attempt
                  WHERE next_attempt.saga_id = a.saga_id
                    AND next_attempt.step = a.step
                    AND next_attempt.attempt_number > a.attempt_number)
            ORDER BY a.deadline_at, a.saga_id
            LIMIT :batchSize OFFSET :offset
            """, nativeQuery = true)
    List<UUID> findDueSagaIds(@Param("now") Instant now,
                              @Param("maxAttempts") int maxAttempts,
                              @Param("batchSize") int batchSize,
                              @Param("offset") int offset);
}
