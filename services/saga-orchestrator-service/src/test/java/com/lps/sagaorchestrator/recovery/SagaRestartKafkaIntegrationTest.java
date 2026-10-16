package com.lps.sagaorchestrator.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.model.Decision;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.sagaorchestrator.kafka.SagaKafkaConfiguration;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaRepository;
import com.lps.sagaorchestrator.saga.SagaStep;
import com.lps.sagaorchestrator.saga.SagaStatus;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.annotation.DirtiesContext;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Stops Kafka listeners while durable brokers remain alive, then checks offset and Inbox recovery. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.recovery.startup-scan-enabled=false"
})
@DirtiesContext(
        classMode = DirtiesContext.ClassMode.AFTER_CLASS
)
class SagaRestartKafkaIntegrationTest {
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    @Container static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired KafkaListenerEndpointRegistry listeners;
    @Autowired ObjectMapper mapper;
    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaOrchestrationService sagaService;
    @Autowired JdbcTemplate jdbc;

    @AfterEach
    void resumeListeners() {
        listeners.start();
    }

    @Test
    void backlogWhileOfflineIsConsumedOnceWhenSameGroupRestarts() throws Exception {
        listeners.stop();
        ApplicationCreatedEvent app = application();
        RecordMetadata first = publish(SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC,
                app.applicationId(), app);

        assertFalse(sagas.existsByApplicationId(app.applicationId()));
        listeners.start();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertTrue(sagas.existsByApplicationId(app.applicationId()));
            assertEquals(1, outbox.countByApplicationId(app.applicationId()));
        });

        // The unchanged consumer group must commit beyond the handled record.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertOffsetCommittedBeyond(SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC, first));

        // A second delivery with the same eventId cannot add a second command.
        RecordMetadata duplicate = publish(SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC,
                app.applicationId(), app);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertOffsetCommittedBeyond(SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC, duplicate);
            Integer inboxCount = jdbc.queryForObject("""
                    SELECT count(*) FROM saga_orchestrator.processed_messages
                    WHERE message_id = ?
                    """, Integer.class, app.eventId());
            assertEquals(1, inboxCount);
        });
        assertEquals(1, outbox.countByApplicationId(app.applicationId()));
    }

    @Test
    void scoringCompletionSurvivesConsumerRestartAndAdvancesSavedSaga() throws Exception {
        ApplicationCreatedEvent app = application();
        sagaService.initialize(app);
        assertEquals(SagaStep.SCORING, sagas.findAll().stream()
                .filter(s -> s.getApplicationId().equals(app.applicationId()))
                .findFirst().orElseThrow().getCurrentStep());
        listeners.stop();
        CreditScoringCompletedEvent scoring = new CreditScoringCompletedEvent(
                UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                app.correlationId(), app.applicationId(), 750, true);
        publish(SagaKafkaConfiguration.SCORING_COMPLETED_TOPIC, app.applicationId(), scoring);

        assertEquals(1, outbox.countByApplicationId(app.applicationId()));
        listeners.start();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saga = sagas.findAll().stream()
                    .filter(s -> s.getApplicationId().equals(app.applicationId()))
                    .findFirst().orElseThrow();
            assertEquals(SagaStep.BUREAU, saga.getCurrentStep());
            assertEquals(750, saga.getCreditScore());
            assertEquals(2, outbox.countByApplicationId(app.applicationId()));
        });
    }

    @Test
    void bureauAndDecisionEventsCompleteWorkflowAcrossTwoListenerRestarts() throws Exception {
        ApplicationCreatedEvent app = application();
        sagaService.initialize(app);
        sagaService.handleCreditScoringCompleted(new CreditScoringCompletedEvent(
                UUID.randomUUID(), "CreditScoringCompleted", Instant.now(),
                app.correlationId(), app.applicationId(), 750, true));

        listeners.stop();
        publish(SagaKafkaConfiguration.BUREAU_COMPLETED_TOPIC, app.applicationId(),
                new BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted",
                        Instant.now(), app.correlationId(), app.applicationId(), 720, false, true));
        listeners.start();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saga = sagas.findAll().stream().filter(s -> s.getApplicationId().equals(app.applicationId()))
                    .findFirst().orElseThrow();
            assertEquals(SagaStep.DECISION, saga.getCurrentStep());
            assertEquals(3, outbox.countByApplicationId(app.applicationId()));
        });

        listeners.stop();
        publish(SagaKafkaConfiguration.DECISION_COMPLETED_TOPIC, app.applicationId(),
                new DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted", Instant.now(),
                        app.correlationId(), app.applicationId(), Decision.APPROVED, "verified"));
        listeners.start();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saga = sagas.findAll().stream().filter(s -> s.getApplicationId().equals(app.applicationId()))
                    .findFirst().orElseThrow();
            assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
            assertEquals(SagaStep.DISBURSEMENT, saga.getCurrentStep());
            assertNull(saga.getCompletedAt());
            assertEquals(4, outbox.countByApplicationId(app.applicationId()));
        });
    }

    /** Admin group offsets show that the same logical consumer group resumed processing. */
    private void assertOffsetCommittedBeyond(String topic, RecordMetadata sent) throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            var committed = admin.listConsumerGroupOffsets(SagaKafkaConfiguration.CONSUMER_GROUP)
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            var offset = committed.get(new TopicPartition(topic, sent.partition()));
            assertNotNull(offset, "No committed offset for resumed consumer partition");
            assertTrue(offset.offset() > sent.offset(), "Record was not committed after processing");
        }
    }

    private RecordMetadata publish(String topic, UUID applicationId, Object event) throws Exception {
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            return producer.send(new ProducerRecord<>(topic, applicationId.toString(),
                    mapper.writeValueAsString(event))).get();
        }
    }

    private static ApplicationCreatedEvent application() {
        return new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("9000.00"), 12, "EUR");
    }
}
