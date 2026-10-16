package com.lps.bureau.kafka;

import com.lps.bureau.config.KafkaConfiguration;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Publishes the bureau result event. */
@Component
public class BureauProducer {
    private final KafkaTemplate<String, BureauCheckCompletedEvent> kafkaTemplate;

    public BureauProducer(KafkaTemplate<String, BureauCheckCompletedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void publish(BureauCheckCompletedEvent event) {
        try {
            // RECORD acknowledgment may follow only successful broker confirmation.
            kafkaTemplate.send(
                    KafkaConfiguration.BUREAU_COMPLETED_TOPIC,
                    event.applicationId().toString(),
                    event
            ).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing completion event", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Completion event not confirmed by Kafka", exception);
        }
    }
}
