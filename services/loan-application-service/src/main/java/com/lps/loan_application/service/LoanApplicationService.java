package com.lps.loan_application.service;

import com.lps.loan_application.cache.LoanApplicationCache;
import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplication;
import com.lps.loan_application.exception.ApplicationNotFoundException;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.loan_application.outbox.OutboxEvent;
import com.lps.loan_application.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.loan_application.repository.LoanApplicationRepository;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Coordinates Loan Application business operations.
 *
 * Creation owns the PostgreSQL transaction that atomically persists the
 * application and its durable Outbox event. Kafka is deliberately outside
 * this transaction.
 */
@Service
public class LoanApplicationService {

    private static final String AGGREGATE_TYPE = "LOAN_APPLICATION";
    private static final String EVENT_TYPE = "ApplicationCreated";

    private final LoanApplicationRepository repository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final LoanApplicationCache cache;
    private final MeterRegistry metrics;

    public LoanApplicationService(
            LoanApplicationRepository repository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper,
            LoanApplicationCache cache,
            MeterRegistry metrics
    ) {
        this.repository = repository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.cache = cache;
        this.metrics = metrics;
    }

    /**
     * Creates the application and its corresponding Outbox event atomically.
     */
    @Transactional
    public LoanApplicationResponse create(
            CreateLoanApplicationRequest request
    ) {

        UUID applicationId = UUID.randomUUID();
        LoanApplication application = new LoanApplication(
                applicationId,
                request.customerId(),
                request.amount(),
                request.term(),
                request.currency()
        );

        LoanApplication saved = repository.save(application);

        ApplicationCreatedEvent event = new ApplicationCreatedEvent(
                UUID.randomUUID(),
                "ApplicationCreated",
                Instant.now(),
                saved.getId(),
                saved.getId(),
                saved.getCustomerId(),
                saved.getAmount(),
                saved.getTerm(),
                saved.getCurrency()
        );

        try {
            String payload = objectMapper.writeValueAsString(event);

            OutboxEvent outboxEvent = new OutboxEvent(
                    event.eventId(),
                    AGGREGATE_TYPE,
                    saved.getId(),
                    EVENT_TYPE,
                    payload,
                    Instant.now()
            );

            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Failed to serialize ApplicationCreatedEvent",
                    exception
            );
        }

        return LoanApplicationResponse.from(saved);
    }

    /**
     * Explicit cache-aside: a Redis HIT bypasses the repository; a MISS or Redis
     * outage falls back to the repository's own read-only transaction.
     * No service-wide transaction stays open during the Redis operations.
     */
    public LoanApplicationResponse getById(UUID applicationId) {
        var lookup = cache.get(applicationId);
        if (lookup.response().isPresent()) {
            return lookup.response().get();
        }

        LoanApplication application = repository.findById(applicationId)
                .orElseThrow(() -> new ApplicationNotFoundException(applicationId));

        LoanApplicationResponse response = LoanApplicationResponse.from(application);
        if (!lookup.available()) {
            // Count a completed PostgreSQL fallback, not merely an attempted cache lookup.
            try { metrics.counter("lps.cache.fallback", "cache", "loan_application").increment(); }
            catch (RuntimeException ignored) { /* Monitoring cannot fail a successful database read. */ }
        }
        if (lookup.available()) {
            cache.put(response);
        }
        return response;
    }
}
