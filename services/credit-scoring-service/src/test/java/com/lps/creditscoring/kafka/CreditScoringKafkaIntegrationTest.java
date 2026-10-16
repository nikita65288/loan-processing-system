package com.lps.creditscoring.kafka;

import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
)
@EmbeddedKafka(
        partitions = 3,
        topics = {"loan.scoring.requested", "loan.scoring.completed"}
)
class CreditScoringKafkaIntegrationTest {
    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void shouldConsumeCommandAndPublishCompletedEvent() {
        // Kafka key — String, а payload — JSON.
        var producerProps = KafkaTestUtils.producerProps(broker);

        producerProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class
        );
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        KafkaTemplate<String, Object> template =
                new KafkaTemplate<>(
                        new DefaultKafkaProducerFactory<>(producerProps)
                );

        UUID applicationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        PerformCreditScoringCommand command = new PerformCreditScoringCommand(
                UUID.randomUUID(),
                "PerformCreditScoring",
                Instant.now(),
                correlationId,
                applicationId,
                UUID.randomUUID(),
                BigDecimal.TEN,
                12,
                "EUR"
        );

        var consumerProps = KafkaTestUtils.consumerProps("mock-service-scoring-output-" + UUID.randomUUID(), "false", broker);
        consumerProps.put("auto.offset.reset", "earliest");
        JsonDeserializer<CreditScoringCompletedEvent> deserializer = new JsonDeserializer<>(CreditScoringCompletedEvent.class);
        deserializer.addTrustedPackages("com.lps.contracts");
        Consumer<String, CreditScoringCompletedEvent> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                consumerProps, new StringDeserializer(), deserializer);

        try {
            consumer.subscribe(java.util.List.of("loan.scoring.completed"));
            template.send("loan.scoring.requested", applicationId.toString(), command).join();
            var record = KafkaTestUtils.getSingleRecord(consumer, "loan.scoring.completed", java.time.Duration.ofSeconds(10));
            assertEquals(applicationId, record.value().applicationId());
            assertEquals(correlationId, record.value().correlationId());
            assertEquals(750, record.value().score());
            assertTrue(record.value().approved());
        } finally {
            consumer.close();
        }
    }
}
