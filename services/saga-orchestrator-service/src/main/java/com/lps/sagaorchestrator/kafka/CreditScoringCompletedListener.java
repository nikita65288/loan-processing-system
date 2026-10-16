package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Adapts a scoring event to the transactional Saga state transition. */
@Component
public class CreditScoringCompletedListener {
    private static final Logger log = LoggerFactory.getLogger(CreditScoringCompletedListener.class);
    private final SagaOrchestrationService service;

    public CreditScoringCompletedListener(SagaOrchestrationService service) { this.service = service; }

    @KafkaListener(topics = SagaKafkaConfiguration.SCORING_COMPLETED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "scoringCompletedListenerFactory")
    public void consume(ConsumerRecord<String, CreditScoringCompletedEvent> record) {
        service.handleCreditScoringCompleted(record.value());
        // Log only after the database transaction has returned successfully.
        log.info("Saga completion event processed topic={}, applicationId={}, partition={}, offset={}",
                record.topic(), record.value().applicationId(), record.partition(), record.offset());
    }
}
