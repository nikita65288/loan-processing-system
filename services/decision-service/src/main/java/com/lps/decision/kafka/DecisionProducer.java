package com.lps.decision.kafka;

import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.decision.config.KafkaConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Publishes the decision result event. */
@Component
public class DecisionProducer {
    private final KafkaTemplate<String, DecisionCompletedEvent> kafkaTemplate;

    public DecisionProducer(KafkaTemplate<String, DecisionCompletedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(DecisionCompletedEvent event) {
        try {
            // RECORD acknowledgment may follow only successful broker confirmation.
            kafkaTemplate.send(
                    KafkaConfiguration.DECISION_COMPLETED_TOPIC,
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
