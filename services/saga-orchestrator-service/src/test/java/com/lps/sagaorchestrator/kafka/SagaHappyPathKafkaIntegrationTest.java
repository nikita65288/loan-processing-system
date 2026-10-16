package com.lps.sagaorchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.*;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.outbox.SagaOutboxPublisher;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import com.lps.sagaorchestrator.saga.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Real Kafka and PostgreSQL exercise every listener and Outbox command with JSON-only headers. */
@Testcontainers
@SpringBootTest(properties = {"lps.scheduling.enabled=false"})
class SagaHappyPathKafkaIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;

    @Test
    void applicationScoringBureauDecisionAndDuplicateEvents() throws Exception {
        UUID appId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        ApplicationCreatedEvent application = new ApplicationCreatedEvent(
                UUID.randomUUID(), "ApplicationCreated", Instant.now(), correlationId, appId,
                UUID.randomUUID(), new BigDecimal("8500.00"), 18, "EUR");

        // The test acts as Kafka event producer; the actual Orchestrator listeners and publisher run.
        try (var producer = new KafkaProducer<String, String>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
             var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "saga-workflow-assert-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {

            consumer.subscribe(List.of(SagaKafkaConfiguration.SCORING_REQUESTED_TOPIC,
                    SagaKafkaConfiguration.BUREAU_REQUESTED_TOPIC,
                    SagaKafkaConfiguration.DECISION_REQUESTED_TOPIC));
            send(producer, SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC, appId, application);
            awaitStep(appId, SagaStep.SCORING);
            await().atMost(Duration.ofSeconds(20)).until(() -> outbox.countByApplicationId(appId) == 1);
            publisher.publishPending();
            var scoring = mapper.readValue(receive(consumer, SagaKafkaConfiguration.SCORING_REQUESTED_TOPIC, appId),
                    PerformCreditScoringCommand.class);
            assertEquals(correlationId, scoring.correlationId());
            assertEquals(application.customerId(), scoring.customerId());

            var scoringCompleted = new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted",
                    Instant.now(), correlationId, appId, 750, true);
            send(producer, SagaKafkaConfiguration.SCORING_COMPLETED_TOPIC, appId, scoringCompleted);
            awaitStep(appId, SagaStep.BUREAU);
            assertEquals(2, outbox.countByApplicationId(appId));
            publisher.publishPending();
            var bureau = mapper.readValue(receive(consumer, SagaKafkaConfiguration.BUREAU_REQUESTED_TOPIC, appId),
                    PerformBureauCheckCommand.class);
            assertEquals(correlationId, bureau.correlationId());
            assertEquals(appId, bureau.applicationId());

            send(producer, SagaKafkaConfiguration.SCORING_COMPLETED_TOPIC, appId, scoringCompleted);
            // Duplicate scoring must not create another bureau command.
            await().atMost(Duration.ofSeconds(10)).until(() -> outbox.countByApplicationId(appId) == 2);

            var bureauCompleted = new BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted",
                    Instant.now(), correlationId, appId, 720, false, true);
            send(producer, SagaKafkaConfiguration.BUREAU_COMPLETED_TOPIC, appId, bureauCompleted);
            awaitStep(appId, SagaStep.DECISION);
            assertEquals(3, outbox.countByApplicationId(appId));
            send(producer, SagaKafkaConfiguration.BUREAU_COMPLETED_TOPIC, appId, bureauCompleted);
            publisher.publishPending();
            var decisionCommand = mapper.readValue(
                    receive(consumer, SagaKafkaConfiguration.DECISION_REQUESTED_TOPIC, appId), MakeDecisionCommand.class);
            assertEquals(750, decisionCommand.creditScore());
            assertEquals(720, decisionCommand.bureauScore());
            assertFalse(decisionCommand.hasDelinquencies());
            assertEquals(correlationId, decisionCommand.correlationId());

            var completed = new DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted",
                    Instant.now(), correlationId, appId, Decision.APPROVED, "PASS");
            send(producer, SagaKafkaConfiguration.DECISION_COMPLETED_TOPIC, appId, completed);
            awaitStep(appId, SagaStep.DISBURSEMENT);
            SagaInstance saga = state(appId);
            assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
            assertEquals(Decision.APPROVED, saga.getFinalDecision());
            assertNull(saga.getCompletedAt());
            assertEquals(4, outbox.countByApplicationId(appId));
            publisher.publishPending();
            assertTrue(outbox.findAll().stream().filter(row -> row.getApplicationId().equals(appId))
                    .allMatch(row -> row.getStatus() == SagaOutboxStatus.PUBLISHED));
            send(producer, SagaKafkaConfiguration.DECISION_COMPLETED_TOPIC, appId, completed);
            await().atMost(Duration.ofSeconds(10)).until(() -> inboxCount() == 4);
            assertEquals(4, outbox.countByApplicationId(appId));
        }
    }

    private long inboxCount() {
        return jdbc.queryForObject("SELECT count(*) FROM saga_orchestrator.processed_messages", Long.class);
    }

    private void send(KafkaProducer<String, String> producer, String topic, UUID id, Object value) throws Exception {
        producer.send(new ProducerRecord<>(topic, id.toString(), mapper.writeValueAsString(value))).get();
    }

    private String receive(KafkaConsumer<String, String> consumer, String topic, UUID id) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            for (var record : consumer.poll(Duration.ofMillis(300))) {
                if (topic.equals(record.topic()) && id.toString().equals(record.key())) {
                    return record.value();
                }
            }
        }
        throw new AssertionError("Missing Kafka command on topic " + topic);
    }

    private void awaitStep(UUID id, SagaStep step) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                .until(() -> sagas.findAll().stream().anyMatch(s ->
                        s.getApplicationId().equals(id) && s.getCurrentStep() == step));
    }

    private SagaInstance state(UUID id) {
        return sagas.findAll().stream().filter(s -> s.getApplicationId().equals(id)).findFirst().orElseThrow();
    }
}
