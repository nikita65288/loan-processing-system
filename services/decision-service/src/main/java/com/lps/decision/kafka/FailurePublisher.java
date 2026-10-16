package com.lps.decision.kafka;

import com.lps.decision.config.KafkaConfiguration;
import com.lps.contracts.event.ProcessingFailureEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Publishes an explicit deterministic failure; Kafka acknowledgement precedes command offset commit. */
@Component
public class FailurePublisher {
    private final KafkaTemplate<String, ProcessingFailureEvent> template;

    public FailurePublisher(KafkaTemplate<String, ProcessingFailureEvent> template) {
        this.template = template;
    }

    public void publish(ProcessingFailureEvent event) {
        try {
            template.send(KafkaConfiguration.DECISION_FAILED_TOPIC, event.applicationId().toString(), event)
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publishing failure event", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failure event not confirmed by Kafka", exception);
        }
    }
}
