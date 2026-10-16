package com.lps.creditscoring.kafka;

import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.creditscoring.config.KafkaConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Publishes the scoring result event without owning workflow orchestration. */
@Component
public class CreditScoringProducer {
    private final KafkaTemplate<String, CreditScoringCompletedEvent> kafkaTemplate;

    public CreditScoringProducer(KafkaTemplate<String, CreditScoringCompletedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(CreditScoringCompletedEvent event) {
        try {
            // RECORD acknowledgment may follow only successful broker confirmation.
            kafkaTemplate.send(
                    KafkaConfiguration.SCORING_COMPLETED_TOPIC,
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
