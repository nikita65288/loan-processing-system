package com.lps.bureau.kafka;

import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.event.BureauCheckCompletedEvent;
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

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {"loan.bureau.check.requested", "loan.bureau.check.completed"})
class BureauKafkaIntegrationTest {
    @Autowired private EmbeddedKafkaBroker broker;

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
        PerformBureauCheckCommand command = new PerformBureauCheckCommand(UUID.randomUUID(), "PerformBureauCheck",
                Instant.now(), correlationId, applicationId, UUID.randomUUID());

        var consumerProps = KafkaTestUtils.consumerProps("mock-service-bureau-output-" + UUID.randomUUID(), "false", broker);
        consumerProps.put("auto.offset.reset", "earliest");
        JsonDeserializer<BureauCheckCompletedEvent> deserializer = new JsonDeserializer<>(BureauCheckCompletedEvent.class);
        deserializer.addTrustedPackages("com.lps.contracts");
        Consumer<String, BureauCheckCompletedEvent> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                consumerProps, new StringDeserializer(), deserializer);
        try {
            consumer.subscribe(java.util.List.of("loan.bureau.check.completed"));
            template.send("loan.bureau.check.requested", applicationId.toString(), command).join();
            var record = KafkaTestUtils.getSingleRecord(consumer, "loan.bureau.check.completed", java.time.Duration.ofSeconds(10));
            assertEquals(applicationId, record.value().applicationId());
            assertEquals(correlationId, record.value().correlationId());
            assertEquals(720, record.value().bureauScore());
            assertFalse(record.value().hasDelinquencies());
            assertTrue(record.value().approved());
        } finally {
            consumer.close();
        }
    }
}
