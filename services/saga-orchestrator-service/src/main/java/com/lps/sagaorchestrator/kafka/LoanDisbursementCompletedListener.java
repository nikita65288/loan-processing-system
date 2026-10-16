package com.lps.sagaorchestrator.kafka;
import com.lps.contracts.event.LoanDisbursementCompletedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
/** Registers completed business result in the Saga Inbox before committing Kafka offset. */
@Component
public class LoanDisbursementCompletedListener {
    private final SagaOrchestrationService orchestrator;
    public LoanDisbursementCompletedListener(SagaOrchestrationService orchestrator) {this.orchestrator=orchestrator;}
    @KafkaListener(topics=SagaKafkaConfiguration.DISBURSEMENT_COMPLETED_TOPIC,
            groupId=SagaKafkaConfiguration.CONSUMER_GROUP,containerFactory="disbursementCompletedListenerFactory")
    public void consume(ConsumerRecord<String,LoanDisbursementCompletedEvent> record) {
        if(record.value()==null || record.value().applicationId()==null
                || !record.value().applicationId().toString().equals(record.key()))
            throw new IllegalArgumentException("Kafka key must be applicationId");
        orchestrator.handleDisbursementCompleted(record.value());
    }
}
