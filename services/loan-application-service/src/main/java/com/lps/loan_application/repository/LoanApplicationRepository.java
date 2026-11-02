package com.lps.loan_application.repository;

import com.lps.loan_application.entity.LoanApplication;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;
import java.util.Optional;
import org.springframework.transaction.annotation.Transactional;

public interface LoanApplicationRepository extends JpaRepository<LoanApplication, UUID> {
    /** Uses a fresh PostgreSQL read after any failed insert transaction has rolled back. */
    @Transactional(readOnly = true)
    Optional<LoanApplication> findByIdempotencyKey(String idempotencyKey);
}
