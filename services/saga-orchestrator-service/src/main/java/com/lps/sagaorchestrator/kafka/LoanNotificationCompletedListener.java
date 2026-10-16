package com.lps.sagaorchestrator.kafka;
import com.lps.contracts.event.LoanNotificationCompletedEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
/** Registers completed business result in the Saga Inbox before committing Kafka offset. */
@Component
public class LoanNotificationCompletedListener {
    private final SagaOrchestrationService orchestrator;
    public LoanNotificationCompletedListener(SagaOrchestrationService orchestrator) {this.orchestrator=orchestrator;}
    @KafkaListener(topics=SagaKafkaConfiguration.NOTIFICATION_COMPLETED_TOPIC,
            groupId=SagaKafkaConfiguration.CONSUMER_GROUP,containerFactory="notificationCompletedListenerFactory")
    public void consume(ConsumerRecord<String,LoanNotificationCompletedEvent> record) {
        if(record.value()==null || record.value().applicationId()==null
                || !record.value().applicationId().toString().equals(record.key()))
            throw new IllegalArgumentException("Kafka key must be applicationId");
        orchestrator.handleNotificationCompleted(record.value());
    }
}
