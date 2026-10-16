package com.lps.loan_application.outbox;

import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@DataJpaTest
class OutboxReliabilityRepositoryIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private LoanApplicationRepository loanApplicationRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        loanApplicationRepository.deleteAll();
    }

    @Test
    void shouldReleaseOnlyStaleClaims() {
        OutboxEvent stale = createEvent();
        stale.markClaimed(Instant.now().minus(2, ChronoUnit.MINUTES));

        OutboxEvent fresh = createEvent();
        fresh.markClaimed(Instant.now());

        outboxEventRepository.save(stale);
        outboxEventRepository.save(fresh);

        int released = outboxEventRepository.releaseStaleClaims(
                OutboxEventStatus.CLAIMED,
                OutboxEventStatus.NEW,
                Instant.now().minus(60, ChronoUnit.SECONDS)
        );

        assertEquals(1, released);
        assertEquals(OutboxEventStatus.NEW,
                outboxEventRepository.findById(stale.getId()).orElseThrow().getStatus());
        assertEquals(OutboxEventStatus.CLAIMED,
                outboxEventRepository.findById(fresh.getId()).orElseThrow().getStatus());
    }

    @Test
    void shouldRecordFailureAndMoveEventToFailedAtRetryLimit() {
        OutboxEvent event = createEvent();
        event.markClaimed(Instant.now());
        outboxEventRepository.save(event);

        Instant claimedAt = event.getClaimedAt();

        int updated = outboxEventRepository.recordPublicationFailure(
                event.getId(),
                "Kafka unavailable",
                1,
                OutboxEventStatus.NEW,
                OutboxEventStatus.FAILED,
                OutboxEventStatus.CLAIMED,
                claimedAt
        );

        assertEquals(1, updated);

        OutboxEvent failed = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertEquals(OutboxEventStatus.FAILED, failed.getStatus());
        assertEquals(1, failed.getRetryCount());
        assertEquals("Kafka unavailable", failed.getLastError());
        assertNull(failed.getClaimedAt());
    }

    @Test
    void shouldDeleteOnlyOldPublishedEvents() {
        OutboxEvent oldPublished = createEvent();
        oldPublished.markClaimed(Instant.now().minus(10, ChronoUnit.DAYS));
        oldPublished.markPublished(Instant.now().minus(8, ChronoUnit.DAYS));

        OutboxEvent recentPublished = createEvent();
        recentPublished.markClaimed(Instant.now());
        recentPublished.markPublished(Instant.now());

        OutboxEvent failed = createEvent();
        failed.markClaimed(Instant.now());
        failed.markPublicationFailed("failure", 10);

        outboxEventRepository.save(oldPublished);
        outboxEventRepository.save(recentPublished);
        outboxEventRepository.save(failed);

        int deleted = outboxEventRepository.deletePublishedBefore(
                OutboxEventStatus.PUBLISHED,
                Instant.now().minus(7, ChronoUnit.DAYS)
        );

        assertEquals(1, deleted);
        assertFalse(outboxEventRepository.findById(oldPublished.getId()).isPresent());
        assertTrue(outboxEventRepository.findById(recentPublished.getId()).isPresent());
        assertTrue(outboxEventRepository.findById(failed.getId()).isPresent());
    }

    private OutboxEvent createEvent() {
        return new OutboxEvent(
                UUID.randomUUID(),
                "LOAN_APPLICATION",
                UUID.randomUUID(),
                "LoanApplicationCreated",
                "{}",
                Instant.now()
        );
    }
}
