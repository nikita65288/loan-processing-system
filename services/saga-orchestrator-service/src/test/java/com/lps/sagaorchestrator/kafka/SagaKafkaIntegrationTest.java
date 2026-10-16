package com.lps.sagaorchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.outbox.SagaOutboxPublisher;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import com.lps.sagaorchestrator.saga.SagaRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.*;

/** Real Kafka + PostgreSQL: inbound event, atomic initialization, outbound command and key. */
@Testcontainers
@SpringBootTest(properties = {"lps.scheduling.enabled=false"})
class SagaKafkaIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void configuration(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired SagaRepository sagas;
    @Autowired SagaOutboxRepository outbox;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired ObjectMapper mapper;

    @Test
    void eventCreatesSagaAndPublishesCompatibleScoringCommand() throws Exception {
        UUID appId = UUID.randomUUID();
        ApplicationCreatedEvent event = new ApplicationCreatedEvent(
                UUID.randomUUID(), "ApplicationCreated", Instant.now(), appId,
                appId, UUID.randomUUID(), new BigDecimal("9999.00"), 12, "EUR");

        // A raw JSON publisher ensures the listener does not rely on Java type headers.
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>(SagaKafkaConfiguration.APPLICATION_CREATED_TOPIC,
                    appId.toString(), mapper.writeValueAsString(event))).get();
        }

        Instant deadline = Instant.now().plusSeconds(15);
        while (!sagas.existsByApplicationId(appId) && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        assertTrue(sagas.existsByApplicationId(appId), "Kafka listener failed to create Saga");
        assertEquals(1, outbox.countByApplicationId(appId));
        UUID commandId = outbox.findAll().stream()
                .filter(row -> row.getApplicationId().equals(appId))
                .findFirst().orElseThrow().getId();

        publisher.publishPending();
        assertEquals(SagaOutboxStatus.PUBLISHED, outbox.findById(commandId).orElseThrow().getStatus());

        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "saga-orchestration-assert-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(SagaKafkaConfiguration.SCORING_REQUESTED_TOPIC));
            deadline = Instant.now().plusSeconds(15);
            boolean found = false;
            while (Instant.now().isBefore(deadline) && !found) {
                var messages = consumer.poll(Duration.ofMillis(500));
                for (var record : messages) {
                    PerformCreditScoringCommand command = mapper.readValue(
                            record.value(), PerformCreditScoringCommand.class);
                    if (!appId.equals(command.applicationId())) continue;
                    assertEquals(appId.toString(), record.key());
                    assertEquals(commandId, command.commandId());
                    assertEquals(event.correlationId(), command.correlationId());
                    found = true;
                }
            }
            assertTrue(found, "The first scoring command was not observed in Kafka");
        }
    }
}
