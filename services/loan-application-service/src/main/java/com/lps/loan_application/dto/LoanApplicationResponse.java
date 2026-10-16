package com.lps.loan_application.dto;

import com.lps.loan_application.entity.LoanApplication;
import com.lps.loan_application.entity.LoanApplicationStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record LoanApplicationResponse(
        UUID id,
        UUID customerId,
        BigDecimal amount,
        Integer term,
        String currency,
        LoanApplicationStatus status,
        Instant createdAt,
        Instant updatedAt
) {

    public static LoanApplicationResponse from(
            LoanApplication application
    ) {
        return new LoanApplicationResponse(
                application.getId(),
                application.getCustomerId(),
                application.getAmount(),
                application.getTerm(),
                application.getCurrency(),
                application.getStatus(),
                application.getCreatedAt(),
                application.getUpdatedAt()
        );
    }
}
