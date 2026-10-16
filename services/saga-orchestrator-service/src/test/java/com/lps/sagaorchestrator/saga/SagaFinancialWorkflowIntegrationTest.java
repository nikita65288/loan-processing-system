package com.lps.sagaorchestrator.saga;

import com.lps.contracts.command.*;
import com.lps.contracts.event.*;
import com.lps.contracts.model.*;
import com.lps.sagaorchestrator.outbox.*;
import com.lps.sagaorchestrator.retry.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import java.time.Instant;
import java.math.BigDecimal;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Financial transitions and invariants under real PostgreSQL row locks. */
@Testcontainers
@SpringBootTest(properties={"lps.scheduling.enabled=false","lps.kafka.consumer.enabled=false",
        "spring.kafka.admin.auto-create=false"})
class SagaFinancialWorkflowIntegrationTest {
    @Container static final PostgreSQLContainer<?> DB=new PostgreSQLContainer<>("postgres:16");
    @DynamicPropertySource static void db(DynamicPropertyRegistry props){
        props.add("spring.datasource.url",DB::getJdbcUrl);
        props.add("spring.datasource.username",DB::getUsername);
        props.add("spring.datasource.password",DB::getPassword);
    }
    @Autowired SagaOrchestrationService service;
    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void cleanup(){
        jdbc.update("DELETE FROM saga_orchestrator.processed_messages");
        outbox.deleteAll();
        sagas.deleteAll();
    }
    private ApplicationCreatedEvent init(){
        UUID app=UUID.randomUUID();Instant now=Instant.now();
        var event=new ApplicationCreatedEvent(UUID.randomUUID(),"ApplicationCreated",now,
                app,app,UUID.randomUUID(),new BigDecimal("8000.00"),12,"EUR");
        service.initialize(event);
        service.handleCreditScoringCompleted(new CreditScoringCompletedEvent(UUID.randomUUID(),
                "CreditScoringCompleted",now,app,app,760,true));
        service.handleBureauCheckCompleted(new BureauCheckCompletedEvent(UUID.randomUUID(),
                "BureauCheckCompleted",now,app,app,740,false,true));
        return event;
    }
    private SagaInstance state(ApplicationCreatedEvent app){
        return sagas.findAll().stream().filter(s->s.getApplicationId().equals(app.applicationId())).findFirst().orElseThrow();
    }
    private SagaStepAttempt attempt(ApplicationCreatedEvent app,SagaStep step){
        return attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(state(app).getSagaId(),step).orElseThrow();
    }
    @Test void approvedHasOnlyOnePayoutAndEndsAfterNotification() throws Exception {
        var app=init();
        var decision=new DecisionCompletedEvent(UUID.randomUUID(),"DecisionCompleted",Instant.now(),
                app.correlationId(),app.applicationId(),Decision.APPROVED,"PASS");
        service.handleDecisionCompleted(decision);service.handleDecisionCompleted(decision);
        assertEquals(SagaStep.DISBURSEMENT,state(app).getCurrentStep());
        assertEquals(4,outbox.countByApplicationId(app.applicationId()));
        var payoutOutbox=outbox.findAll().stream().filter(r->r.getCommandType().equals("DisburseLoan")).findFirst().orElseThrow();
        var command=mapper.readValue(payoutOutbox.getPayload(),DisburseLoanCommand.class);
        assertEquals(new BigDecimal("8000.00"),command.amount());
        UUID payout=UUID.randomUUID();
        var event=new LoanDisbursementCompletedEvent(UUID.randomUUID(),"LoanDisbursementCompleted",
                Instant.now(),app.correlationId(),app.applicationId(),command.commandId(),payout,app.amount(),app.currency());
        service.handleDisbursementCompleted(event);service.handleDisbursementCompleted(event);
        assertEquals(SagaStep.NOTIFICATION,state(app).getCurrentStep());
        assertEquals(payout,state(app).getDisbursementId());
        assertEquals(5,outbox.countByApplicationId(app.applicationId()));
        var notif=attempt(app,SagaStep.NOTIFICATION);
        service.handleNotificationCompleted(new LoanNotificationCompletedEvent(UUID.randomUUID(),
                "LoanNotificationCompleted",Instant.now(),app.correlationId(),app.applicationId(),
                notif.getCommandId(),NotificationType.LOAN_APPROVED_AND_DISBURSED));
        assertEquals(SagaStatus.COMPLETED,state(app).getStatus());
        assertEquals("SENT_SIMULATED",state(app).getNotificationStatus());
    }
    @Test void rejectionNeverCreatesDisbursementCommand(){
        var app=init();
        service.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),"DecisionCompleted",
                Instant.now(),app.correlationId(),app.applicationId(),Decision.REJECTED,"FAIL_RULES"));
        assertEquals(SagaStep.NOTIFICATION,state(app).getCurrentStep());
        assertEquals(0,outbox.findAll().stream().filter(e->e.getCommandType().equals("DisburseLoan")).count());
        var notification=attempt(app,SagaStep.NOTIFICATION);
        service.handleNotificationCompleted(new LoanNotificationCompletedEvent(UUID.randomUUID(),
                "LoanNotificationCompleted",Instant.now(),app.correlationId(),app.applicationId(),
                notification.getCommandId(),NotificationType.LOAN_REJECTED));
        assertEquals(SagaStatus.COMPLETED,state(app).getStatus());
    }
    @Test void unknownOutcomeAndNotificationFailurePolicies(){
        var app=init();
        service.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),"DecisionCompleted",
                Instant.now(),app.correlationId(),app.applicationId(),Decision.APPROVED,"PASS"));
        // The deadline scheduler calls the same durable transition after attempts are exhausted.
        var saga=state(app);
        saga.requireReconciliation(Instant.now());
        sagas.save(saga);
        assertEquals(SagaStatus.REQUIRES_RECONCILIATION,state(app).getStatus());
        assertEquals("UNKNOWN",state(app).getDisbursementStatus());
        assertEquals(4,outbox.countByApplicationId(app.applicationId()));
        var rejection=init();
        service.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),"DecisionCompleted",
                Instant.now(),rejection.correlationId(),rejection.applicationId(),Decision.REJECTED,"FAIL_RULES"));
        var notification=attempt(rejection,SagaStep.NOTIFICATION);
        service.handleFailure(new LoanNotificationFailedEvent(UUID.randomUUID(),"LoanNotificationFailed",
                Instant.now(),rejection.correlationId(),rejection.applicationId(),notification.getCommandId(),
                "STUB_FAILURE",false),SagaStep.NOTIFICATION);
        assertEquals(SagaStatus.COMPLETED,state(rejection).getStatus());
        assertEquals("FAILED",state(rejection).getNotificationStatus());
    }
}
