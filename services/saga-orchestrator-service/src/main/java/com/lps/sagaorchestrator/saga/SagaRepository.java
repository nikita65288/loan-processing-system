package com.lps.sagaorchestrator.saga;

import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/** The unique application id identifies a Saga; row locking serializes transitions. */
public interface SagaRepository extends JpaRepository<SagaInstance, UUID> {
    boolean existsByApplicationId(UUID applicationId);

    /** Non-locking read for the status endpoint; transitions use a separate locked query. */
    @Transactional(readOnly = true)
    Optional<SagaInstance> findByApplicationId(UUID applicationId);

    /** Bounded, read-only diagnostic scan; no Saga row is locked or mutated. */
    Slice<SagaInstance> findByStatus(SagaStatus status, Pageable pageable);

    /** SELECT FOR UPDATE protects check+mutate+outbox-insert within one transaction. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaInstance s WHERE s.sagaId = :sagaId")
    Optional<SagaInstance> findByIdForUpdate(@Param("sagaId") UUID sagaId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaInstance s WHERE s.applicationId = :applicationId")
    Optional<SagaInstance> findByApplicationIdForUpdate(@Param("applicationId") UUID applicationId);
}
