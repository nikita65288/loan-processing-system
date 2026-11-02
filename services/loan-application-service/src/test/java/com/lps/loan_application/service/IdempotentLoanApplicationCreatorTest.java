package com.lps.loan_application.service;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplication;
import com.lps.loan_application.exception.IdempotencyKeyConflictException;
import com.lps.loan_application.repository.LoanApplicationRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotentLoanApplicationCreatorTest {
    @Mock LoanApplicationService service;
    @Mock LoanApplicationRepository repository;
    private final RequestFingerprint fingerprint = new RequestFingerprint();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();

    private CreateLoanApplicationRequest request() {
        return new CreateLoanApplicationRequest(UUID.randomUUID(), new BigDecimal("100.00"), 12, "EUR");
    }

    private IdempotentLoanApplicationCreator creator() {
        return new IdempotentLoanApplicationCreator(service, repository, fingerprint, metrics);
    }

    @Test
    void noHeaderPreservesLegacyCreation() {
        var req = request();
        creator().create(req, null);
        verify(service).create(req);
        verifyNoInteractions(repository);
    }

    @Test
    void sameKeyAndPayloadReplaysWithoutCreatingAnything() {
        var req = request();
        var original = new LoanApplication(UUID.randomUUID(), req.customerId(), req.amount(), req.term(), req.currency());
        original.assignIdempotency("key", fingerprint.forCreation(req));
        when(repository.findByIdempotencyKey("key")).thenReturn(Optional.of(original));
        LoanApplicationResponse result = creator().create(req, "key");
        assertEquals(original.getId(), result.id());
        verify(service, never()).create(any(), any(), any());
    }

    @Test
    void sameKeyWithOtherPayloadIsRejected() {
        var req = request();
        var original = new LoanApplication(UUID.randomUUID(), req.customerId(), req.amount(), req.term(), req.currency());
        original.assignIdempotency("key", fingerprint.forCreation(req));
        var changed = new CreateLoanApplicationRequest(req.customerId(), new BigDecimal("200.00"), req.term(), req.currency());
        when(repository.findByIdempotencyKey("key")).thenReturn(Optional.of(original));
        assertThrows(IdempotencyKeyConflictException.class, () -> creator().create(changed, "key"));
        verifyNoInteractions(service);
    }

    @Test
    void concurrentUniqueLossReplaysOnlyAfterRolledBackTransaction() {
        var req = request();
        var original = new LoanApplication(UUID.randomUUID(), req.customerId(), req.amount(), req.term(), req.currency());
        original.assignIdempotency("key", fingerprint.forCreation(req));
        when(repository.findByIdempotencyKey("key"))
                .thenReturn(Optional.empty(), Optional.of(original));
        var duplicate = new SQLException("duplicate key value violates unique constraint \"uq_loan_applications_idempotency_key\"", "23505");
        when(service.create(eq(req), eq("key"), anyString()))
                .thenThrow(new DataIntegrityViolationException("duplicate", duplicate));
        assertEquals(original.getId(), creator().create(req, "key").id());
        verify(repository, times(2)).findByIdempotencyKey("key");
    }

    @Test
    void unrelatedSqlFailureIsNeverReplayed() {
        var req = request();
        when(repository.findByIdempotencyKey("key")).thenReturn(Optional.empty());
        var unrelated = new SQLException("duplicate key value violates unique constraint \"pk_outbox_events\"", "23505");
        when(service.create(eq(req), eq("key"), anyString()))
                .thenThrow(new DataIntegrityViolationException("unrelated", unrelated));
        assertThrows(DataIntegrityViolationException.class, () -> creator().create(req, "key"));
        verify(repository, times(1)).findByIdempotencyKey("key");
    }

    @Test
    void winnerMissingAfterExpectedUniqueFailureMustNotReturnSuccess() {
        var req = request();
        when(repository.findByIdempotencyKey("key")).thenReturn(Optional.empty());
        var duplicate = new SQLException("duplicate key value violates unique constraint \"uq_loan_applications_idempotency_key\"", "23505");
        when(service.create(eq(req), eq("key"), anyString()))
                .thenThrow(new DataIntegrityViolationException("duplicate", duplicate));
        assertThrows(DataIntegrityViolationException.class, () -> creator().create(req, "key"));
    }
}
