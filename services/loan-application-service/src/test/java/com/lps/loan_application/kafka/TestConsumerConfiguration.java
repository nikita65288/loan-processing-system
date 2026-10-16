package com.lps.loan_application.kafka;

import com.lps.contracts.event.ApplicationCreatedEvent;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@TestConfiguration
public class TestConsumerConfiguration {

    @KafkaListener(
            topics = "${lps.kafka.topic.application-created}",
            groupId = "${lps.kafka.test.consumer.group-id}",
            autoStartup = "true"
    )
    public void consume(ApplicationCreatedEvent event) {
        RECEIVED_EVENTS.add(event);
    }

    private static final BlockingQueue<ApplicationCreatedEvent> RECEIVED_EVENTS =
            new LinkedBlockingQueue<>();

    public static BlockingQueue<ApplicationCreatedEvent> receivedEvents() {
        return RECEIVED_EVENTS;
    }

    public static void clearEvents() {
        RECEIVED_EVENTS.clear();
    }
}
