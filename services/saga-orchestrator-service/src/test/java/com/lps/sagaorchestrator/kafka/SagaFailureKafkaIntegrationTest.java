package com.lps.sagaorchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.BureauCheckFailedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.sagaorchestrator.retry.SagaStepAttemptRepository;
import com.lps.sagaorchestrator.saga.*;
import org.apache.kafka.clients.producer.*;
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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Kafka JSON deserialization, real listener, Inbox and durable failure transition. */
@Testcontainers
@SpringBootTest(properties = { "lps.scheduling.enabled=false" })
class SagaFailureKafkaIntegrationTest {
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    @Container static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired SagaOrchestrationService orchestrator;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;

    @Test
    void bureauPermanentFailureArrivesFromKafkaAndDuplicateIsDeduplicated() throws Exception {
        var app = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", Instant.now(),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("9000"), 12, "EUR");
        orchestrator.initialize(app);
        orchestrator.handleCreditScoringCompleted(new CreditScoringCompletedEvent(UUID.randomUUID(),
                "CreditScoringCompleted", Instant.now(), app.correlationId(), app.applicationId(), 750, true));
        var saga = state(app.applicationId());
        UUID commandId = attempts.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(saga.getSagaId(), SagaStep.BUREAU)
                .orElseThrow().getCommandId();
        var failure = new BureauCheckFailedEvent(UUID.randomUUID(), "BureauCheckFailed", Instant.now(),
                app.correlationId(), app.applicationId(), commandId, "BUREAU_INVALID_REQUEST", false);
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            var record = new ProducerRecord<>(SagaKafkaConfiguration.BUREAU_FAILED_TOPIC, app.applicationId().toString(),
                    mapper.writeValueAsString(failure));
            producer.send(record).get();
            await().atMost(Duration.ofSeconds(20)).until(() -> state(app.applicationId()).getStatus() == SagaStatus.FAILED);
            producer.send(record).get();
            await().atMost(Duration.ofSeconds(20)).until(() -> inboxCount(failure.eventId()) == 1);
            assertEquals(1, inboxCount(failure.eventId()));
            assertEquals("BUREAU_INVALID_REQUEST", state(app.applicationId()).getFailureReason());
            assertEquals(SagaStep.BUREAU, state(app.applicationId()).getCurrentStep());
        }
    }

    private SagaInstance state(UUID appId) {
        return sagas.findAll().stream().filter(s -> appId.equals(s.getApplicationId())).findFirst().orElseThrow();
    }

    private int inboxCount(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM saga_orchestrator.processed_messages WHERE message_id=?",
                Integer.class, eventId);
    }
}
