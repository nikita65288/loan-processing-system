package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Adapts a bureau event to the transactional Saga state transition. */
@Component
public class BureauCheckCompletedListener {
    private static final Logger log = LoggerFactory.getLogger(BureauCheckCompletedListener.class);
    private final SagaOrchestrationService service;

    public BureauCheckCompletedListener(SagaOrchestrationService service) { this.service = service; }

    @KafkaListener(
            topics = SagaKafkaConfiguration.BUREAU_COMPLETED_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "bureauCompletedListenerFactory"
    )
    public void consume(ConsumerRecord<String, BureauCheckCompletedEvent> record) {
        service.handleBureauCheckCompleted(record.value());
        // Log only after the database transaction has returned successfully.
        log.info("Saga completion event processed topic={}, applicationId={}, partition={}, offset={}",
                record.topic(), record.value().applicationId(), record.partition(), record.offset());
    }
}
