package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ApplicationCreatedConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(ApplicationCreatedConsumer.class);

    private final boolean failOnEvent;

    public ApplicationCreatedConsumer(
            @Value("${lps.kafka.consumer.fail-on-event:false}")
            boolean failOnEvent
    ) {
        this.failOnEvent = failOnEvent;
    }

    @KafkaListener(
            topics = KafkaConfiguration.APPLICATION_CREATED_TOPIC,
            groupId = KafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, ApplicationCreatedEvent> record
    ) {
        ApplicationCreatedEvent event = record.value();

        log.info(
                "Consumed ApplicationCreatedEvent " +
                        "eventId={}, applicationId={}, key={}, " +
                        "partition={}, offset={}, consumerGroup={}",
                event.eventId(),
                event.applicationId(),
                record.key(),
                record.partition(),
                record.offset(),
                KafkaConfiguration.CONSUMER_GROUP
        );

        if (failOnEvent) {
            throw new IllegalStateException(
                    "Intentional Kafka consumer failure"
            );
        }
    }
}
