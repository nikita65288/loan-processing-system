package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.CreditScoringFailedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Passes explicit scoring failures to the transactional Saga state machine. */
@Component
public class CreditScoringFailedListener {
    private final SagaOrchestrationService orchestrator;
    public CreditScoringFailedListener(SagaOrchestrationService orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topics = SagaKafkaConfiguration.SCORING_FAILED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "scoringFailedListenerFactory")
    public void consume(ConsumerRecord<String, CreditScoringFailedEvent> record) {
        if (record.value() == null || record.value().applicationId() == null
                || !record.value().applicationId().toString().equals(record.key())) {
            throw new IllegalArgumentException("Kafka failure key must equal applicationId");
        }
        orchestrator.handleFailure(record.value(), SagaStep.SCORING);
    }
}
