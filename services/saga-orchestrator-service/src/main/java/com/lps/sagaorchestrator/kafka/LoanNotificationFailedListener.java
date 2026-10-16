package com.lps.sagaorchestrator.kafka;
import com.lps.contracts.event.LoanNotificationFailedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
/** Routes typed technical failure to the shared transactional Saga failure policy. */
@Component
public class LoanNotificationFailedListener {
    private final SagaOrchestrationService orchestrator;
    public LoanNotificationFailedListener(SagaOrchestrationService orchestrator) {this.orchestrator=orchestrator;}
    @KafkaListener(topics=SagaKafkaConfiguration.NOTIFICATION_FAILED_TOPIC,
            groupId=SagaKafkaConfiguration.CONSUMER_GROUP,containerFactory="notificationFailedListenerFactory")
    public void consume(ConsumerRecord<String,LoanNotificationFailedEvent> record) {
        if(record.value()==null || record.value().applicationId()==null
                || !record.value().applicationId().toString().equals(record.key()))
            throw new IllegalArgumentException("Kafka key must be applicationId");
        orchestrator.handleFailure(record.value(),SagaStep.NOTIFICATION);
    }
}
