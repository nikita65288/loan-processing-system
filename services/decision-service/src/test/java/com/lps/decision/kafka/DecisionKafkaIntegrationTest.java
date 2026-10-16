package com.lps.decision.kafka;

import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.model.Decision;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {"loan.decision.requested", "loan.decision.completed"})
class DecisionKafkaIntegrationTest {
    @Autowired private EmbeddedKafkaBroker broker;

    @Test
    void shouldConsumeCommandAndPublishApprovedDecision() {
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
        MakeDecisionCommand command = new MakeDecisionCommand(UUID.randomUUID(), "MakeDecision", Instant.now(),
                correlationId, applicationId, 750, 720, false);

        var consumerProps = KafkaTestUtils.consumerProps("mock-service-decision-output-" + UUID.randomUUID(), "false", broker);
        consumerProps.put("auto.offset.reset", "earliest");
        JsonDeserializer<DecisionCompletedEvent> deserializer = new JsonDeserializer<>(DecisionCompletedEvent.class);
        deserializer.addTrustedPackages("com.lps.contracts");
        Consumer<String, DecisionCompletedEvent> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                consumerProps, new StringDeserializer(), deserializer);
        try {
            consumer.subscribe(java.util.List.of("loan.decision.completed"));
            template.send("loan.decision.requested", applicationId.toString(), command).join();
            var record = KafkaTestUtils.getSingleRecord(consumer, "loan.decision.completed", java.time.Duration.ofSeconds(10));
            assertEquals(applicationId, record.value().applicationId());
            assertEquals(correlationId, record.value().correlationId());
            assertEquals(Decision.APPROVED, record.value().decision());
            assertEquals("PASS", record.value().reason());
        } finally {
            consumer.close();
        }
    }
}
