package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplicationCreatedConsumerTest {

    @Test
    void shouldProcessEvent() {

        ApplicationCreatedConsumer consumer =
                new ApplicationCreatedConsumer(false);

        ApplicationCreatedEvent event =
                createEvent();

        ConsumerRecord<String, ApplicationCreatedEvent> record =
                new ConsumerRecord<>(
                        KafkaConfiguration.APPLICATION_CREATED_TOPIC,
                        0,
                        42L,
                        event.applicationId().toString(),
                        event
                );

        assertDoesNotThrow(
                () -> consumer.consume(record)
        );
    }

    @Test
    void shouldFailWhenFailureModeEnabled() {

        ApplicationCreatedConsumer consumer =
                new ApplicationCreatedConsumer(true);

        ApplicationCreatedEvent event =
                createEvent();

        ConsumerRecord<String, ApplicationCreatedEvent> record =
                new ConsumerRecord<>(
                        KafkaConfiguration.APPLICATION_CREATED_TOPIC,
                        0,
                        42L,
                        event.applicationId().toString(),
                        event
                );

        assertThrows(
                IllegalStateException.class,
                () -> consumer.consume(record)
        );
    }

    private ApplicationCreatedEvent createEvent() {
        return new ApplicationCreatedEvent(
                UUID.randomUUID(),
                "ApplicationCreated",
                Instant.now(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                BigDecimal.valueOf(10000),
                12,
                "EUR"
        );
    }
}
