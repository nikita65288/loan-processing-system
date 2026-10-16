package com.lps.loandisbursement.disbursement;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;
/** Unique application_id constraint also protects concurrent service instances. */
public interface DisbursementRepository extends JpaRepository<DisbursementRecord,UUID> {
    Optional<DisbursementRecord> findByApplicationId(UUID applicationId);
}
