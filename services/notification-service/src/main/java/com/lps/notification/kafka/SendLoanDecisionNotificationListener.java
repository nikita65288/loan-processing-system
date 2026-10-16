package com.lps.notification.kafka;
import com.lps.contracts.command.SendLoanDecisionNotificationCommand;
import com.lps.notification.delivery.NotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
/** Kafka offsets advance after the durable simulated-delivery transaction. */
@Component
public class SendLoanDecisionNotificationListener {
    private final NotificationService service;
    public SendLoanDecisionNotificationListener(NotificationService service){this.service=service;}
    @KafkaListener(topics=LocalKafkaConfiguration.REQUESTED,containerFactory="commandListenerFactory")
    public void consume(ConsumerRecord<String,SendLoanDecisionNotificationCommand> record){
        if(record.value()==null || record.value().applicationId()==null
                || !record.value().applicationId().toString().equals(record.key()))
            throw new IllegalArgumentException("Notification key must match applicationId");
        service.process(record.value());
    }
}
