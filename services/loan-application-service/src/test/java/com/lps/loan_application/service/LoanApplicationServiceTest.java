package com.lps.loan_application.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.lps.loan_application.cache.LoanApplicationCache;
import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplication;
import com.lps.loan_application.entity.LoanApplicationStatus;
import com.lps.loan_application.exception.ApplicationNotFoundException;
import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.outbox.OutboxEvent;
import com.lps.loan_application.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoanApplicationServiceTest {

    @Mock
    private LoanApplicationRepository repository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private LoanApplicationCache cache;

    @Mock
    private io.micrometer.core.instrument.MeterRegistry metrics;

    @InjectMocks
    private LoanApplicationService service;

    @Test
    void shouldCreateApplication() throws JsonProcessingException {

        UUID customerId = UUID.randomUUID();

        CreateLoanApplicationRequest request =
                new CreateLoanApplicationRequest(
                        customerId,
                        new BigDecimal("10000.00"),
                        12,
                        "EUR"
                );

        when(repository.save(any(LoanApplication.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        when(objectMapper.writeValueAsString(any()))
                .thenReturn("{\"eventId\":\"event\"}");

        LoanApplicationResponse response =
                service.create(request);

        assertNotNull(response.id());
        assertEquals(customerId, response.customerId());
        assertEquals(new BigDecimal("10000.00"), response.amount());
        assertEquals(12, response.term());
        assertEquals("EUR", response.currency());
        assertEquals(
                LoanApplicationStatus.CREATED,
                response.status()
        );

        verify(repository).save(any(LoanApplication.class));
        verify(outboxEventRepository).save(any(OutboxEvent.class));
    }

    @Test
    void shouldGetExistingApplication() {

        UUID applicationId = UUID.randomUUID();

        LoanApplication application =
                new LoanApplication(
                        applicationId,
                        UUID.randomUUID(),
                        new BigDecimal("10000.00"),
                        12,
                        "EUR"
                );

        when(cache.get(applicationId)).thenReturn(
                new LoanApplicationCache.Lookup(Optional.empty(), true));
        when(repository.findById(applicationId))
                .thenReturn(Optional.of(application));

        LoanApplicationResponse response =
                service.getById(applicationId);

        assertEquals(
                application.getCustomerId(),
                response.customerId()
        );

        verify(repository).findById(applicationId);
        verify(cache).put(any(LoanApplicationResponse.class));
    }

    @Test
    void shouldThrowWhenApplicationDoesNotExist() {

        UUID applicationId = UUID.randomUUID();

        when(cache.get(applicationId)).thenReturn(
                new LoanApplicationCache.Lookup(Optional.empty(), true));
        when(repository.findById(applicationId))
                .thenReturn(Optional.empty());

        assertThrows(
                ApplicationNotFoundException.class,
                () -> service.getById(applicationId)
        );

        verify(repository).findById(applicationId);
        verify(cache, never()).put(any());
    }

    @Test
    void shouldUseCacheHitWithoutDatabaseLookup() {
        UUID id = UUID.randomUUID();
        LoanApplicationResponse value = new LoanApplicationResponse(
                id, UUID.randomUUID(), new BigDecimal("1234.50"), 24,
                "EUR", LoanApplicationStatus.CREATED,
                java.time.Instant.parse("2026-10-12T10:00:00Z"),
                java.time.Instant.parse("2026-10-12T10:00:00Z")
        );
        when(cache.get(id)).thenReturn(
                new LoanApplicationCache.Lookup(Optional.of(value), true));

        assertEquals(value, service.getById(id));
        verify(repository, never()).findById(any());
        verify(cache, never()).put(any());
    }

    @Test
    void shouldSkipSecondRedisOperationWhenLookupUnavailable() {
        UUID id = UUID.randomUUID();
        LoanApplication application = new LoanApplication(id, UUID.randomUUID(),
                new BigDecimal("2500.00"), 12, "EUR");
        when(cache.get(id)).thenReturn(
                new LoanApplicationCache.Lookup(Optional.empty(), false));
        when(repository.findById(id)).thenReturn(Optional.of(application));

        assertEquals(id, service.getById(id).id());
        verify(cache, never()).put(any());
        verify(repository).findById(id);
    }

}