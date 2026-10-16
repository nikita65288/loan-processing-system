package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Completes a Saga after Decision Service has made its business decision. */
@Component
public class DecisionCompletedListener {
    private static final Logger log = LoggerFactory.getLogger(DecisionCompletedListener.class);
    private final SagaOrchestrationService service;

    public DecisionCompletedListener(SagaOrchestrationService service) { this.service = service; }

    @KafkaListener(topics = SagaKafkaConfiguration.DECISION_COMPLETED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "decisionCompletedListenerFactory")
    public void consume(ConsumerRecord<String, DecisionCompletedEvent> record) {
        service.handleDecisionCompleted(record.value());
        // Log only after the database transaction has returned successfully.
        log.info("Saga completion event processed topic={}, applicationId={}, partition={}, offset={}",
                record.topic(), record.value().applicationId(), record.partition(), record.offset());
    }
}
