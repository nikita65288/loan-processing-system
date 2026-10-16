package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.BureauCheckFailedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Passes explicit bureau failures to the transactional Saga state machine. */
@Component
public class BureauCheckFailedListener {
    private final SagaOrchestrationService orchestrator;
    public BureauCheckFailedListener(SagaOrchestrationService orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topics = SagaKafkaConfiguration.BUREAU_FAILED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "bureauFailedListenerFactory")
    public void consume(ConsumerRecord<String, BureauCheckFailedEvent> record) {
        if (record.value() == null || record.value().applicationId() == null
                || !record.value().applicationId().toString().equals(record.key())) {
            throw new IllegalArgumentException("Kafka failure key must equal applicationId");
        }
        orchestrator.handleFailure(record.value(), SagaStep.BUREAU);
    }
}
