package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.DecisionFailedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Passes explicit decision failures to the transactional Saga state machine. */
@Component
public class DecisionFailedListener {
    private final SagaOrchestrationService orchestrator;
    public DecisionFailedListener(SagaOrchestrationService orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topics = SagaKafkaConfiguration.DECISION_FAILED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "decisionFailedListenerFactory")
    public void consume(ConsumerRecord<String, DecisionFailedEvent> record) {
        if (record.value() == null || record.value().applicationId() == null
                || !record.value().applicationId().toString().equals(record.key())) {
            throw new IllegalArgumentException("Kafka failure key must equal applicationId");
        }
        orchestrator.handleFailure(record.value(), SagaStep.DECISION);
    }
}
