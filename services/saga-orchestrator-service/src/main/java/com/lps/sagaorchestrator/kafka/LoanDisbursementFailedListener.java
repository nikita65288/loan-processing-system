package com.lps.sagaorchestrator.kafka;
import com.lps.contracts.event.LoanDisbursementFailedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
/** Routes typed technical failure to the shared transactional Saga failure policy. */
@Component
public class LoanDisbursementFailedListener {
    private final SagaOrchestrationService orchestrator;
    public LoanDisbursementFailedListener(SagaOrchestrationService orchestrator) {this.orchestrator=orchestrator;}
    @KafkaListener(topics=SagaKafkaConfiguration.DISBURSEMENT_FAILED_TOPIC,
            groupId=SagaKafkaConfiguration.CONSUMER_GROUP,containerFactory="disbursementFailedListenerFactory")
    public void consume(ConsumerRecord<String,LoanDisbursementFailedEvent> record) {
        if(record.value()==null || record.value().applicationId()==null
                || !record.value().applicationId().toString().equals(record.key()))
            throw new IllegalArgumentException("Kafka key must be applicationId");
        orchestrator.handleFailure(record.value(),SagaStep.DISBURSEMENT);
    }
}
