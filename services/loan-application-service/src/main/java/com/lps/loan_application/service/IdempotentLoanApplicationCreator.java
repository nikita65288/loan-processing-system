package com.lps.loan_application.service;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplication;
import com.lps.loan_application.exception.IdempotencyKeyConflictException;
import com.lps.loan_application.repository.LoanApplicationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Optional;

/** Coordinates HTTP idempotency OUTSIDE the transactional create method. */
@Service
public class IdempotentLoanApplicationCreator {
    private static final String KEY_CONSTRAINT = "uq_loan_applications_idempotency_key";
    private final LoanApplicationService applicationService;
    private final LoanApplicationRepository repository;
    private final RequestFingerprint fingerprint;
    private final MeterRegistry metrics;

    public IdempotentLoanApplicationCreator(LoanApplicationService applicationService,
                                            LoanApplicationRepository repository,
                                            RequestFingerprint fingerprint,
                                            MeterRegistry metrics) {
        this.applicationService = applicationService;
        this.repository = repository;
        this.fingerprint = fingerprint;
        this.metrics = metrics;
    }

    /** Replays the committed create response or atomically creates one new operation. */
    public LoanApplicationResponse create(CreateLoanApplicationRequest request, String key) {
        if (key == null) {
            return applicationService.create(request);
        }
        String requestHash = fingerprint.forCreation(request);
        Optional<LoanApplication> existing = repository.findByIdempotencyKey(key);
        if (existing.isPresent()) {
            return replay(existing.get(), requestHash);
        }
        try {
            LoanApplicationResponse created = applicationService.create(request, key, requestHash);
            count("created");
            return created;
        } catch (DataIntegrityViolationException exception) {
            // PostgreSQL has resolved the concurrent unique insert. The failed
            // transaction has already rolled back before we re-read the winner.
            if (!isOurUniqueConstraint(exception)) {
                throw exception;
            }
            LoanApplication winner = repository.findByIdempotencyKey(key)
                    .orElseThrow(() -> exception); // Never invent success if the row is unavailable.
            return replay(winner, requestHash);
        }
    }

    private LoanApplicationResponse replay(LoanApplication original, String requestHash) {
        if (!requestHash.equals(original.getRequestFingerprint())) {
            count("conflict");
            throw new IdempotencyKeyConflictException();
        }
        count("replayed");
        return LoanApplicationResponse.from(original);
    }

    /** Never mask unrelated NOT NULL, foreign-key, or Outbox uniqueness errors. */
    private boolean isOurUniqueConstraint(DataIntegrityViolationException exception) {
        boolean duplicate = false;
        boolean correctConstraint = false;
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof SQLException sql) {
                duplicate |= "23505".equals(sql.getSQLState());
            }
            if (cause instanceof org.hibernate.exception.ConstraintViolationException hibernate) {
                correctConstraint |= KEY_CONSTRAINT.equals(hibernate.getConstraintName());
            }
            // PostgreSQL error text contains the quoted constraint identifier.
            correctConstraint |= cause.getMessage() != null
                    && cause.getMessage().contains("\"" + KEY_CONSTRAINT + "\"");
            cause = cause.getCause();
        }
        return duplicate && correctConstraint;
    }

    private void count(String outcome) {
        try {
            metrics.counter("lps.http.idempotency", "outcome", outcome).increment();
        } catch (RuntimeException ignored) {
            // Telemetry must never change HTTP idempotency behavior.
        }
    }
}
