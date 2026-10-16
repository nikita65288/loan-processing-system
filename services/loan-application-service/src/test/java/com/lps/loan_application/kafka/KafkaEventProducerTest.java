package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;


class KafkaEventProducerTest {

    private final KafkaTemplate<String, ApplicationCreatedEvent> kafkaTemplate =
            mock(KafkaTemplate.class);

    private final KafkaEventProducer producer =
            new KafkaEventProducer(kafkaTemplate);

    @Test
    void shouldPublishEventUsingApplicationIdAsKey() throws InterruptedException {

        UUID applicationId = UUID.randomUUID();

        ApplicationCreatedEvent event =
                new ApplicationCreatedEvent(
                        UUID.randomUUID(),
                        "ApplicationCreated",
                        java.time.Instant.now(),
                        applicationId,
                        applicationId,
                        UUID.randomUUID(),
                        BigDecimal.valueOf(10000),
                        12,
                        "EUR"
                );

        CompletableFuture<SendResult<String, ApplicationCreatedEvent>> future =
                new CompletableFuture<>();

        when(kafkaTemplate.send(
                KafkaConfiguration.APPLICATION_CREATED_TOPIC,
                applicationId.toString(),
                event
        )).thenReturn(future);

        producer.publish(event);

        verify(kafkaTemplate).send(
                KafkaConfiguration.APPLICATION_CREATED_TOPIC,
                applicationId.toString(),
                event
        );
    }
}
