package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaEventProducer {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, ApplicationCreatedEvent> kafkaTemplate;

    public KafkaEventProducer(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, ApplicationCreatedEvent> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, ApplicationCreatedEvent>> publish(
            ApplicationCreatedEvent event
    ) {
        log.info(
                "Publishing ApplicationCreatedEvent eventId={}, applicationId={}, topic={}",
                event.eventId(),
                event.applicationId(),
                KafkaConfiguration.APPLICATION_CREATED_TOPIC
        );

        return kafkaTemplate.send(
                KafkaConfiguration.APPLICATION_CREATED_TOPIC,
                event.applicationId().toString(),
                event
        );
    }
}
