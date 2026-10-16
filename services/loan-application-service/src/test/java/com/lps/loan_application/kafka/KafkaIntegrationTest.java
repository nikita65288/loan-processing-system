package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(
        properties = {
                "spring.datasource.password=postgres",
                "lps.scheduling.enabled=false",
                "spring.datasource.hikari.connection-timeout=2000"
        }
)
@Testcontainers
@Import(TestConsumerConfiguration.class)
class KafkaIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka:3.8.1")
            );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {

        registry.add(
                "spring.kafka.bootstrap-servers",
                KAFKA::getBootstrapServers
        );

        registry.add(
                "lps.kafka.topic.application-created",
                () -> "loan.application.created"
        );

        registry.add(
                "lps.kafka.test.consumer.group-id",
                () -> "kafka-integration-test-" + UUID.randomUUID()
        );
    }

    @Autowired
    private KafkaEventProducer kafkaEventProducer;

    @BeforeEach
    void setUp() {
        TestConsumerConfiguration.clearEvents();
    }

    @AfterEach
    void tearDown() {
        TestConsumerConfiguration.clearEvents();
    }

    @Test
    void shouldPublishAndConsumeApplicationCreatedEvent() throws Exception {

        UUID applicationId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();

        ApplicationCreatedEvent event = new ApplicationCreatedEvent(
                UUID.randomUUID(),
                "ApplicationCreated",
                Instant.now(),
                applicationId,
                applicationId,
                customerId,
                new BigDecimal("10000.00"),
                12,
                "EUR"
        );

        kafkaEventProducer.publish(event);

        ApplicationCreatedEvent receivedEvent =
                TestConsumerConfiguration.receivedEvents()
                        .poll(10, TimeUnit.SECONDS);

        assertNotNull(
                receivedEvent,
                "Kafka consumer did not receive the event within 10 seconds"
        );

        assertEquals(event.eventId(), receivedEvent.eventId());
        assertEquals(event.applicationId(), receivedEvent.applicationId());
        assertEquals(event.customerId(), receivedEvent.customerId());
        assertEquals(event.amount(), receivedEvent.amount());
        assertEquals(event.term(), receivedEvent.term());
        assertEquals(event.currency(), receivedEvent.currency());
        assertEquals(event.occurredAt(), receivedEvent.occurredAt());
    }
}
