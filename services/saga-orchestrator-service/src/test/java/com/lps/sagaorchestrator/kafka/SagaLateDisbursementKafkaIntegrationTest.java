package com.lps.sagaorchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.outbox.SagaOutboxPublisher;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import com.lps.sagaorchestrator.retry.SagaRetryService;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import com.lps.sagaorchestrator.saga.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.sql.Timestamp;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Real Kafka listener recovers the disbursement timeout and publishes the next Outbox command. */
@Testcontainers
@SpringBootTest(properties = {"lps.scheduling.enabled=false", "lps.saga.retry.max-attempts=1"})
class SagaLateDisbursementKafkaIntegrationTest {
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    @Container static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired SagaOrchestrationService orchestrator;
    @Autowired SagaRetryService retry;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;

    @Test
    void kafkaLateSuccessIsAcknowledgedAfterRecoveryAndNotificationOutboxPersists() throws Exception {
        UUID appId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Instant now = Instant.now();
        var application = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", now,
                correlationId, appId, UUID.randomUUID(), new BigDecimal("7300.00"), 18, "EUR");
        orchestrator.initialize(application);
        orchestrator.handleCreditScoringCompleted(new CreditScoringCompletedEvent(UUID.randomUUID(),
                "CreditScoringCompleted", now, correlationId, appId, 770, true));
        orchestrator.handleBureauCheckCompleted(new BureauCheckCompletedEvent(UUID.randomUUID(),
                "BureauCheckCompleted", now, correlationId, appId, 730, false, true));
        orchestrator.handleDecisionCompleted(new DecisionCompletedEvent(UUID.randomUUID(),
                "DecisionCompleted", now, correlationId, appId, Decision.APPROVED, "PASS"));

        var saga = sagas.findByApplicationId(appId).orElseThrow();
        UUID commandId = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(saga.getSagaId(),
                SagaStep.DISBURSEMENT).orElseThrow().getCommandId();
        Instant expired = Instant.now().minusSeconds(90);
        jdbc.update("""
                UPDATE saga_orchestrator.saga_step_attempts
                SET published_at=?, deadline_at=?, retry_after_at=?
                WHERE saga_id=? AND step='DISBURSEMENT'
                """,
                Timestamp.from(expired.minusSeconds(60)),
                Timestamp.from(expired),
                Timestamp.from(expired.plusSeconds(5)),
                saga.getSagaId());
        retry.process(saga.getSagaId());
        assertEquals(SagaStatus.REQUIRES_RECONCILIATION,
                sagas.findByApplicationId(appId).orElseThrow().getStatus());

        UUID payoutId = UUID.randomUUID();
        var completed = new LoanDisbursementCompletedEvent(UUID.randomUUID(),
                "LoanDisbursementCompleted", Instant.now(), correlationId, appId,
                commandId, payoutId, application.amount(), application.currency());
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            var record = new ProducerRecord<>(SagaKafkaConfiguration.DISBURSEMENT_COMPLETED_TOPIC,
                    appId.toString(), mapper.writeValueAsString(completed));
            producer.send(record).get();
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                var updated = sagas.findByApplicationId(appId).orElseThrow();
                return updated.getStatus() == SagaStatus.IN_PROGRESS
                        && updated.getCurrentStep() == SagaStep.NOTIFICATION;
            });
            assertEquals(payoutId, sagas.findByApplicationId(appId).orElseThrow().getDisbursementId());
            assertEquals(1, countCommands(appId, "SendLoanDecisionNotification"));
            producer.send(record).get();
            var equivalentResult = new LoanDisbursementCompletedEvent(UUID.randomUUID(),
                    "LoanDisbursementCompleted", Instant.now(), correlationId, appId,
                    commandId, payoutId, application.amount(), application.currency());
            producer.send(new ProducerRecord<>(SagaKafkaConfiguration.DISBURSEMENT_COMPLETED_TOPIC,
                    appId.toString(), mapper.writeValueAsString(equivalentResult))).get();
            // One partition key preserves order: when the second ID is processed, replay is also behind us.
            await().atMost(Duration.ofSeconds(20)).until(() -> inboxCount(equivalentResult.eventId()) == 1);
            assertEquals(1, inboxCount(completed.eventId()));
            assertEquals(1, countCommands(appId, "SendLoanDecisionNotification"));

            publisher.publishPending();
            await().atMost(Duration.ofSeconds(20)).until(() -> outbox.findAll().stream()
                    .filter(e -> appId.equals(e.getApplicationId()))
                    .filter(e -> "SendLoanDecisionNotification".equals(e.getCommandType()))
                    .allMatch(e -> e.getStatus() == SagaOutboxStatus.PUBLISHED));
        }
    }

    private long countCommands(UUID appId, String type) {
        return outbox.findAll().stream().filter(e -> appId.equals(e.getApplicationId()))
                .filter(e -> type.equals(e.getCommandType())).count();
    }

    private int inboxCount(UUID eventId) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM saga_orchestrator.processed_messages WHERE message_id=?
                """, Integer.class, eventId);
    }
}
