package com.lps.loandisbursement.kafka;
import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.loandisbursement.disbursement.DisbursementService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
/** Confirms Kafka offset only after the local PostgreSQL business transaction. */
@Component
public class DisburseLoanCommandListener {
    private final DisbursementService service;
    public DisburseLoanCommandListener(DisbursementService service) {this.service=service;}
    @KafkaListener(topics=LocalKafkaConfiguration.REQUESTED,containerFactory="commandListenerFactory")
    public void consume(ConsumerRecord<String,DisburseLoanCommand> record) {
        if (record.value()==null || record.value().applicationId()==null
                || !record.value().applicationId().toString().equals(record.key()))
            throw new IllegalArgumentException("Command Kafka key must equal applicationId");
        service.process(record.value());
    }
}
