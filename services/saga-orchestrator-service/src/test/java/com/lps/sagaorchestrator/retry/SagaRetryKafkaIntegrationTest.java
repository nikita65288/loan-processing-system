package com.lps.sagaorchestrator.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.MessageIdentity;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.sagaorchestrator.kafka.SagaKafkaConfiguration;
import com.lps.sagaorchestrator.outbox.SagaOutboxPublisher;
import com.lps.sagaorchestrator.saga.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/** Actual Kafka delivery preserves logical command ID across physical Saga business retries. */
@Testcontainers
@SpringBootTest(properties = {
        "lps.scheduling.enabled=false",
        "lps.saga.timeout.scoring=2s",
        "lps.saga.retry.backoff=1s",
        "lps.saga.retry.max-attempts=3"
})
class SagaRetryKafkaIntegrationTest {
    private static final Instant T0 = Instant.parse("2026-10-12T09:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired SagaOrchestrationService orchestrator;
    @Autowired SagaRetryService retry;
    @Autowired SagaOutboxPublisher publisher;
    @Autowired SagaRepository sagas;
    @Autowired SagaStepAttemptRepository attempts;
    @Autowired ObjectMapper mapper;
    @MockitoBean Clock clock;

    @Test
    void twoKafkaScoringCommandsHaveSameLogicalIdentityAndLateResponseAdvancesOnce() throws Exception {
        when(clock.instant()).thenReturn(T0);
        var event = new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", T0,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("4200.00"), 12, "EUR");
        orchestrator.initialize(event);
        UUID sagaId = sagas.findAll().stream().filter(s -> s.getApplicationId().equals(event.applicationId()))
                .findFirst().orElseThrow().getSagaId();

        try (var producer = new KafkaProducer<String, String>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
             var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "retry-policy-retry-assert-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(SagaKafkaConfiguration.SCORING_REQUESTED_TOPIC));
            publisher.publishPending();
            var first = mapper.readValue(receive(consumer, event.applicationId()), PerformCreditScoringCommand.class);
            when(clock.instant()).thenReturn(T0.plusSeconds(3));
            retry.process(sagaId);
            publisher.publishPending();
            var second = mapper.readValue(receive(consumer, event.applicationId()), PerformCreditScoringCommand.class);
            assertEquals(first.commandId(), second.commandId());
            assertEquals(first, second); // Retry must not silently alter occurredAt or business payload.
            assertEquals(2, attempts.findAll().stream().filter(a -> a.getSagaId().equals(sagaId)).count());

            var completion = new CreditScoringCompletedEvent(
                    MessageIdentity.resultEventId(first.commandId(), "CreditScoringCompleted"),
                    "CreditScoringCompleted", T0.plusSeconds(4),
                    event.correlationId(), event.applicationId(), 750, true);
            producer.send(new ProducerRecord<>(SagaKafkaConfiguration.SCORING_COMPLETED_TOPIC,
                    event.applicationId().toString(), mapper.writeValueAsString(completion))).get();
            await().atMost(Duration.ofSeconds(20)).until(() ->
                    sagas.findById(sagaId).orElseThrow().getCurrentStep() == SagaStep.BUREAU);
            assertEquals(SagaStatus.IN_PROGRESS, sagas.findById(sagaId).orElseThrow().getStatus());
            assertEquals(2, attempts.findAll().stream().filter(a -> a.getSagaId().equals(sagaId)
                    && a.getStep() == SagaStep.SCORING).count());
        }
    }

    private String receive(KafkaConsumer<String, String> consumer, UUID applicationId) {
        Instant until = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(until)) {
            for (var record : consumer.poll(Duration.ofMillis(300))) {
                if (applicationId.toString().equals(record.key())) return record.value();
            }
        }
        throw new AssertionError("No scoring command received from Kafka");
    }
}
