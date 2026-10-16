package com.lps.sagaorchestrator.inbox;

import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.saga.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** PostgreSQL integration tests validate Inbox uniqueness and transactional rollback, not mocks. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"
})
class InboxIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired ProcessedMessageDao inbox;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;
    @Autowired SagaOrchestrationService sagaService;
    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;

    @BeforeEach
    void clear() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }

    private long count() {
        return jdbc.queryForObject("SELECT count(*) FROM saga_orchestrator.processed_messages", Long.class);
    }

    @Test
    void inboxIsAtomicAndRollbackAllowsRetry() {
        UUID id = UUID.randomUUID();
        assertThrows(IllegalStateException.class, () -> tx.execute(status -> {
            assertTrue(inbox.register(id, "CreditScoringCompleted"));
            throw new IllegalStateException("transaction rolls back");
        }));
        assertEquals(0, count());
        assertTrue(tx.execute((TransactionCallback<Boolean>) status -> inbox.register(id, "CreditScoringCompleted")));
        assertFalse(tx.execute((TransactionCallback<Boolean>) status -> inbox.register(id, "CreditScoringCompleted")));
        assertTrue(tx.execute((TransactionCallback<Boolean>) status -> inbox.register(UUID.randomUUID(), "CreditScoringCompleted")));
        assertEquals(2, count());
    }

    @Test
    void allFourMessagesAreDurableAndDuplicatesDoNotReapplyTransitions() {
        ApplicationCreatedEvent app = application();
        CreditScoringCompletedEvent scoring = scoring(app, 750);
        BureauCheckCompletedEvent bureau = bureau(app, 720, false);
        DecisionCompletedEvent decision = decision(app, Decision.APPROVED);

        sagaService.initialize(app);
        sagaService.initialize(app);
        sagaService.handleCreditScoringCompleted(scoring);
        sagaService.handleCreditScoringCompleted(scoring);
        sagaService.handleBureauCheckCompleted(bureau);
        sagaService.handleBureauCheckCompleted(bureau);
        sagaService.handleDecisionCompleted(decision);
        Instant completedAt = sagas.findAll().get(0).getCompletedAt();
        sagaService.handleDecisionCompleted(decision);

        assertEquals(1, sagas.count());
        assertEquals(SagaStep.DISBURSEMENT, sagas.findAll().get(0).getCurrentStep());
        assertEquals(completedAt, sagas.findAll().get(0).getCompletedAt());
        assertEquals(4, outbox.count());
        assertEquals(4, count());

        // Different event IDs with the same business results are safe semantic replays.
        sagaService.initialize(new ApplicationCreatedEvent(UUID.randomUUID(), app.eventType(),
                Instant.now(), app.correlationId(), app.applicationId(), app.customerId(),
                app.amount(), app.term(), app.currency()));
        sagaService.handleCreditScoringCompleted(scoring(app, 750));
        sagaService.handleBureauCheckCompleted(bureau(app, 720, false));
        sagaService.handleDecisionCompleted(decision(app, Decision.APPROVED));
        assertEquals(8, count());
        assertEquals(4, outbox.count());
        assertEquals(completedAt, sagas.findAll().get(0).getCompletedAt());
    }

    @Test
    void semanticConflictsAndUnknownSagaRollBackInbox() {
        ApplicationCreatedEvent app = application();
        sagaService.initialize(app);
        long baseline = count();
        assertThrows(IllegalStateException.class, () -> sagaService.handleDecisionCompleted(decision(app, Decision.APPROVED)));
        assertThrows(IllegalStateException.class, () -> sagaService.handleCreditScoringCompleted(
                new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                        app.correlationId(), UUID.randomUUID(), 700, true)));
        assertThrows(IllegalStateException.class, () -> sagaService.initialize(
                new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(), app.correlationId(),
                        app.applicationId(), app.customerId(), new BigDecimal("1.00"), app.term(), app.currency())));
        assertEquals(baseline, count());
        sagaService.handleCreditScoringCompleted(scoring(app, 750));
        baseline = count();
        assertThrows(IllegalStateException.class, () -> sagaService.handleCreditScoringCompleted(scoring(app, 100)));
        assertEquals(baseline, count());
        assertEquals(750, sagas.findAll().get(0).getCreditScore());
        assertEquals(2, outbox.count());
    }

    @Test
    void concurrentIdenticalMessageIdWaitsForCommitAndDoesNotRegisterTwice() throws Exception {
        UUID id = UUID.randomUUID();
        CountDownLatch firstInserted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = pool.submit(() -> tx.execute(status -> {
                boolean inserted = inbox.register(id, "CreditScoringCompleted");
                firstInserted.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("timeout");
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
                return inserted;
            }));
            assertTrue(firstInserted.await(10, TimeUnit.SECONDS));
            Future<Boolean> second = pool.submit(() -> tx.execute(status -> {
                secondStarted.countDown();
                return inbox.register(id, "CreditScoringCompleted");
            }));
            assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
            // The other transaction must wait on the unique-key conflict.
            assertThrows(TimeoutException.class, () -> second.get(150, TimeUnit.MILLISECONDS));
            release.countDown();
            assertTrue(first.get(10, TimeUnit.SECONDS));
            assertFalse(second.get(10, TimeUnit.SECONDS));
            assertEquals(1, count());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    private static ApplicationCreatedEvent application() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("12000.00"), 24, "EUR");
    }
    private static CreditScoringCompletedEvent scoring(ApplicationCreatedEvent a, int score) {
        return new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), score, true);
    }
    private static BureauCheckCompletedEvent bureau(ApplicationCreatedEvent a, int score, boolean delinquent) {
        return new BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), score, delinquent, true);
    }
    private static DecisionCompletedEvent decision(ApplicationCreatedEvent a, Decision result) {
        return new DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), result, "PASS");
    }
}
