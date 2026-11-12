package com.lps.sagaorchestrator.kafka;

import com.lps.contracts.event.DisbursementStatusInquiryResultEvent;
import com.lps.sagaorchestrator.saga.SagaOrchestrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Applies validated inquiry replies in the same Inbox/Saga/Outbox transaction as ordinary events. */
@Component
public class DisbursementStatusInquiryResultListener {
    private final SagaOrchestrationService orchestration;

    public DisbursementStatusInquiryResultListener(SagaOrchestrationService orchestration) {
        this.orchestration = orchestration;
    }

    @KafkaListener(topics = SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_RESULT_TOPIC,
            groupId = SagaKafkaConfiguration.CONSUMER_GROUP,
            containerFactory = "disbursementInquiryResultListenerFactory")
    public void consume(ConsumerRecord<String, DisbursementStatusInquiryResultEvent> record) {
        if (record.value() == null || record.value().applicationId() == null
                || record.value().eventId() == null || record.value().correlationId() == null
                || record.value().eventType() == null || record.value().occurredAt() == null
                || record.value().inquiryId() == null || record.value().disbursementCommandId() == null
                || record.value().status() == null
                || !"DisbursementStatusInquiryResult".equals(record.value().eventType())
                || !record.value().applicationId().toString().equals(record.key()))
            throw new com.lps.kafka.reliability.InvalidKafkaRecordException("Result key must equal applicationId");
        orchestration.handleDisbursementInquiryResult(record.value());
    }
}
