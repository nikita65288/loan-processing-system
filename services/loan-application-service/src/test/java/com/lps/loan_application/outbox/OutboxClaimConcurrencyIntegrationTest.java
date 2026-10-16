package com.lps.loan_application.outbox;

import com.lps.loan_application.repository.LoanApplicationRepository;
import com.lps.loan_application.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxClaimConcurrencyIntegrationTest {

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

    @Autowired
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        loanApplicationRepository.deleteAll();
    }

    @Test
    void shouldDistributeLockedRowsBetweenConcurrentClaimers() throws Exception {
        for (int i = 0; i < 10; i++) {
            outboxEventRepository.save(createOutboxEvent(i));
        }

        outboxEventRepository.flush();

        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<Set<UUID>> first = executor.submit(() -> claimBatchWithBarrier(barrier));
            Future<Set<UUID>> second = executor.submit(() -> claimBatchWithBarrier(barrier));

            Set<UUID> firstBatch = first.get(10, TimeUnit.SECONDS);
            Set<UUID> secondBatch = second.get(10, TimeUnit.SECONDS);

            Set<UUID> intersection = new HashSet<>(firstBatch);
            intersection.retainAll(secondBatch);

            assertTrue(intersection.isEmpty());
            assertEquals(10, firstBatch.size() + secondBatch.size());
            assertEquals(
                    10,
                    outboxEventRepository.findAll().stream()
                            .filter(event -> event.getStatus() == OutboxEventStatus.CLAIMED)
                            .count()
            );
        } finally {
            executor.shutdownNow();
        }
    }

    private Set<UUID> claimBatchWithBarrier(CyclicBarrier barrier) {
        return transactionTemplate.execute(status -> {
            List<OutboxEvent> events =
                    outboxEventRepository.findEventsForUpdateSkipLocked(10, OutboxEventStatus.NEW.name());

            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }

            Instant claimedAt = Instant.now();
            events.forEach(event -> event.markClaimed(claimedAt));
            outboxEventRepository.saveAll(events);

            return events.stream()
                    .map(OutboxEvent::getId)
                    .collect(java.util.stream.Collectors.toSet());
        });
    }

    private OutboxEvent createOutboxEvent(int index) {
        return new OutboxEvent(
                UUID.randomUUID(),
                "LOAN_APPLICATION",
                UUID.randomUUID(),
                "LoanApplicationCreated",
                "{}",
                Instant.now().plusMillis(index)
        );
    }
}
