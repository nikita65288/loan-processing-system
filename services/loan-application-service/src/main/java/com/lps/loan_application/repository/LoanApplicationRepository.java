package com.lps.loan_application.repository;

import com.lps.loan_application.entity.LoanApplication;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface LoanApplicationRepository extends JpaRepository<LoanApplication, UUID> {
}
