package com.lps.loandisbursement.kafka;

import com.lps.contracts.command.DisbursementStatusInquiryCommand;
import com.lps.loandisbursement.disbursement.DisbursementStatusInquiryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes read-only requests; offset follows durable Inbox/Outbox commit. */
@Component
public class DisbursementStatusInquiryListener {
    private final DisbursementStatusInquiryService inquiries;

    public DisbursementStatusInquiryListener(DisbursementStatusInquiryService inquiries) {
        this.inquiries = inquiries;
    }

    @KafkaListener(topics = LocalKafkaConfiguration.INQUIRY_REQUESTED,
            containerFactory = "inquiryListenerFactory")
    public void consume(ConsumerRecord<String, DisbursementStatusInquiryCommand> record) {
        if (record.value() == null || record.value().applicationId() == null
                || record.value().inquiryId() == null || record.value().disbursementCommandId() == null || record.value().correlationId() == null || record.value().occurredAt() == null || !"DisbursementStatusInquiry".equals(record.value().commandType())
                || !record.value().applicationId().toString().equals(record.key()))
            throw new com.lps.kafka.reliability.InvalidKafkaRecordException("Inquiry key must equal applicationId");
        inquiries.inspect(record.value());
    }
}
