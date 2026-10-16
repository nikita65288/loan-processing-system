package com.lps.sagaorchestrator.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/** Real PostgreSQL covers state/outbox atomicity, invalid transitions and concurrent duplicates. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"
})
class SagaHappyPathIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired SagaOrchestrationService service;
    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired com.lps.sagaorchestrator.retry.SagaStepAttemptRepository stepAttempts;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @MockitoSpyBean SagaOutboxRepository outboxSpy;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }

    @Test
    void scoringAndBureauUsePersistedContextAndAvoidDuplicateCommands() throws Exception {
        ApplicationCreatedEvent app = application();
        service.initialize(app);
        service.handleCreditScoringCompleted(scoring(app, 742));
        SagaInstance saga = state(app);
        assertEquals(SagaStep.BUREAU, saga.getCurrentStep());
        assertEquals(742, saga.getCreditScore());
        assertEquals(2, outbox.countByApplicationId(app.applicationId()));
        SagaOutboxEvent bureauRecord = outbox.findAll().stream()
                .filter(r -> r.getCommandType().equals("PerformBureauCheck")).findFirst().orElseThrow();
        PerformBureauCheckCommand bureau = mapper.readValue(bureauRecord.getPayload(), PerformBureauCheckCommand.class);
        assertEquals(bureauRecord.getId(), bureau.commandId());
        assertEquals(app.customerId(), bureau.customerId());
        assertEquals(app.correlationId(), bureau.correlationId());

        assertThrows(IllegalStateException.class, () -> service.handleCreditScoringCompleted(scoring(app, 100))); // conflicting stale result
        assertEquals(2, outbox.countByApplicationId(app.applicationId()));
        assertEquals(742, state(app).getCreditScore());

        service.handleBureauCheckCompleted(bureau(app, 710, true));
        assertEquals(SagaStep.DECISION, state(app).getCurrentStep());
        assertEquals(710, state(app).getBureauScore());
        assertTrue(state(app).getHasDelinquencies());
        SagaOutboxEvent decisionRecord = outbox.findAll().stream()
                .filter(r -> r.getCommandType().equals("MakeDecision")).findFirst().orElseThrow();
        MakeDecisionCommand decision = mapper.readValue(decisionRecord.getPayload(), MakeDecisionCommand.class);
        assertEquals(decisionRecord.getId(), decision.commandId());
        assertEquals(app.applicationId(), decision.applicationId());
        assertEquals(app.correlationId(), decision.correlationId());
        assertEquals(742, decision.creditScore());
        assertEquals(710, decision.bureauScore());
        assertTrue(decision.hasDelinquencies());
        assertThrows(IllegalStateException.class, () -> service.handleBureauCheckCompleted(bureau(app, 200, false)));
        assertEquals(3, outbox.countByApplicationId(app.applicationId()));
    }

    @Test
    void bothBusinessDecisionsCompleteWithoutNewCommand() {
        for (Decision decision : Decision.values()) {
            ApplicationCreatedEvent app = application();
            service.initialize(app);
            service.handleCreditScoringCompleted(scoring(app, 750));
            service.handleBureauCheckCompleted(bureau(app, 710, false));
            service.handleDecisionCompleted(completion(app, decision));
            SagaInstance saga = state(app);
            assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
            assertEquals(decision==Decision.APPROVED?SagaStep.DISBURSEMENT:SagaStep.NOTIFICATION,
                    saga.getCurrentStep());
            assertEquals(decision, saga.getFinalDecision());
            assertEquals("verified", saga.getDecisionReason());
            assertNull(saga.getCompletedAt());
            assertEquals(4, outbox.countByApplicationId(app.applicationId()));
            service.handleDecisionCompleted(completion(app, decision));
            assertEquals(4, outbox.countByApplicationId(app.applicationId()));
            if (decision==Decision.APPROVED) {
                var payout = attemptsFor(app, SagaStep.DISBURSEMENT);
                service.handleDisbursementCompleted(new LoanDisbursementCompletedEvent(UUID.randomUUID(),
                        "LoanDisbursementCompleted",Instant.now(),app.correlationId(),app.applicationId(),
                        payout.getCommandId(),UUID.randomUUID(),app.amount(),app.currency()));
            }
            var notify=attemptsFor(app,SagaStep.NOTIFICATION);
            service.handleNotificationCompleted(new LoanNotificationCompletedEvent(UUID.randomUUID(),
                    "LoanNotificationCompleted",Instant.now(),app.correlationId(),app.applicationId(),
                    notify.getCommandId(),decision==Decision.APPROVED
                    ?com.lps.contracts.model.NotificationType.LOAN_APPROVED_AND_DISBURSED
                    :com.lps.contracts.model.NotificationType.LOAN_REJECTED));
            assertEquals(SagaStatus.COMPLETED,state(app).getStatus());
        }
    }

    @Test
    void unexpectedOrUnknownEventsNeverChangeState() {
        ApplicationCreatedEvent app = application();
        service.initialize(app);
        assertThrows(IllegalStateException.class, () -> service.handleBureauCheckCompleted(bureau(app, 710, false)));
        assertThrows(IllegalStateException.class, () -> service.handleDecisionCompleted(completion(app, Decision.APPROVED)));
        assertThrows(IllegalStateException.class,
                () -> service.handleCreditScoringCompleted(new CreditScoringCompletedEvent(
                        UUID.randomUUID(), "CreditScoringCompleted", Instant.now(), app.correlationId(),
                        UUID.randomUUID(), 700, true)));
        assertThrows(NullPointerException.class, () -> service.handleCreditScoringCompleted(scoring(app, null)));
        assertEquals(SagaStep.SCORING, state(app).getCurrentStep());
        assertEquals(1, outbox.countByApplicationId(app.applicationId()));
    }

    @Test
    void outboxInsertFailureRollsBackScoringTransition() {
        ApplicationCreatedEvent app = application();
        service.initialize(app);
        doThrow(new IllegalStateException("simulated outbox failure")).when(outboxSpy).save(any());
        assertThrows(IllegalStateException.class, () -> service.handleCreditScoringCompleted(scoring(app, 740)));
        assertEquals(SagaStep.SCORING, state(app).getCurrentStep());
        assertNull(state(app).getCreditScore());
        assertEquals(1, outbox.countByApplicationId(app.applicationId()));
        assertEquals(1L, processedCount()); // failed transition rolls back Inbox too
    }

    @Test
    void twoConcurrentCompletionsProduceOnlyOneNextCommand() throws Exception {
        ApplicationCreatedEvent app = application();
        service.initialize(app);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Callable<Void> task = () -> {
                start.await();
                service.handleCreditScoringCompleted(scoring(app, 745));
                return null;
            };
            Future<Void> first = pool.submit(task);
            Future<Void> second = pool.submit(task);
            start.countDown();
            first.get(20, TimeUnit.SECONDS);
            second.get(20, TimeUnit.SECONDS);
            assertEquals(SagaStep.BUREAU, state(app).getCurrentStep());
            assertEquals(2, outbox.countByApplicationId(app.applicationId()));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void twoConcurrentExactDuplicatesCreateOnlyOneBureauCommand() throws Exception {
        ApplicationCreatedEvent app = application();
        service.initialize(app);
        CreditScoringCompletedEvent event = scoring(app, 745);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Callable<Void> task = () -> {
                start.await();
                service.handleCreditScoringCompleted(event);
                return null;
            };
            Future<Void> a = pool.submit(task);
            Future<Void> b = pool.submit(task);
            start.countDown();
            a.get(20, TimeUnit.SECONDS);
            b.get(20, TimeUnit.SECONDS);
            assertEquals(2, outbox.countByApplicationId(app.applicationId()));
            assertEquals(2L, processedCount());
        } finally {
            pool.shutdownNow();
        }
    }

    private com.lps.sagaorchestrator.retry.SagaStepAttempt attemptsFor(ApplicationCreatedEvent app,SagaStep step) {
        return stepAttempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                state(app).getSagaId(),step).orElseThrow();
    }

    private long processedCount() {
        return jdbc.queryForObject("SELECT count(*) FROM saga_orchestrator.processed_messages", Long.class);
    }

    private SagaInstance state(ApplicationCreatedEvent event) {
        return sagas.findAll().stream()
                .filter(s -> s.getApplicationId().equals(event.applicationId())).findFirst().orElseThrow();
    }

    private static ApplicationCreatedEvent application() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("12000.00"), 24, "EUR");
    }
    private static CreditScoringCompletedEvent scoring(ApplicationCreatedEvent a, Integer score) {
        return new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), score, true);
    }
    private static BureauCheckCompletedEvent bureau(ApplicationCreatedEvent a, int score, boolean delinquencies) {
        return new BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), score, delinquencies, true);
    }
    private static DecisionCompletedEvent completion(ApplicationCreatedEvent a, Decision decision) {
        return new DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted", Instant.now(),
                a.correlationId(), a.applicationId(), decision, "verified");
    }
}
