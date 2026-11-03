package com.lps.loandisbursement.disbursement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.MessageIdentity;
import com.lps.contracts.command.DisbursementStatusInquiryCommand;
import com.lps.contracts.event.DisbursementInquiryStatus;
import com.lps.contracts.event.DisbursementStatusInquiryResultEvent;
import com.lps.loandisbursement.kafka.LocalKafkaConfiguration;
import com.lps.loandisbursement.outbox.LocalOutboxEvent;
import com.lps.loandisbursement.outbox.LocalOutboxRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Objects;

/** Inbox, committed PostgreSQL lookup and reply Outbox are one transaction; never executes a payout. */
@Service
public class DisbursementStatusInquiryService {
    private final JdbcTemplate jdbc;
    private final DisbursementRepository records;
    private final LocalOutboxRepository outbox;
    private final ObjectMapper mapper;

    public DisbursementStatusInquiryService(JdbcTemplate jdbc, DisbursementRepository records,
                                             LocalOutboxRepository outbox, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.records = records;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    @Transactional
    public void inspect(DisbursementStatusInquiryCommand request) {
        Objects.requireNonNull(request, "Missing inquiry");
        Objects.requireNonNull(request.inquiryId(), "Missing inquiry ID");
        Objects.requireNonNull(request.applicationId(), "Missing application ID");
        Objects.requireNonNull(request.correlationId(), "Missing correlation ID");
        Objects.requireNonNull(request.disbursementCommandId(), "Missing payout command ID");
        if (!"DisbursementStatusInquiry".equals(request.commandType()) || request.occurredAt() == null)
            throw new IllegalArgumentException("Invalid inquiry contract");

        int inserted = jdbc.update("""
                INSERT INTO loan_disbursement.processed_status_inquiries
                    (inquiry_id, application_id, disbursement_command_id, correlation_id, created_at)
                VALUES (?, ?, ?, ?, now()) ON CONFLICT (inquiry_id) DO NOTHING
                """, request.inquiryId(), request.applicationId(),
                request.disbursementCommandId(), request.correlationId());
        if (inserted == 0) {
            Boolean same = jdbc.queryForObject("""
                    SELECT application_id = ? AND disbursement_command_id = ? AND correlation_id = ?
                    FROM loan_disbursement.processed_status_inquiries WHERE inquiry_id = ?
                    """, Boolean.class, request.applicationId(), request.disbursementCommandId(),
                    request.correlationId(), request.inquiryId());
            if (!Boolean.TRUE.equals(same)) throw new IllegalStateException("Conflicting inquiry ID reuse");
            return; // Original response Outbox was inserted atomically with the Inbox record.
        }

        var existing = records.findByApplicationId(request.applicationId());
        DisbursementInquiryStatus result = existing.isEmpty() ? DisbursementInquiryStatus.NOT_FOUND
                : existing.get().getCommandId().equals(request.disbursementCommandId())
                    ? DisbursementInquiryStatus.COMPLETED : DisbursementInquiryStatus.CONFLICT;
        var payout = result == DisbursementInquiryStatus.COMPLETED ? existing.orElseThrow() : null;
        Instant now = Instant.now();
        var event = new DisbursementStatusInquiryResultEvent(
                MessageIdentity.resultEventId(request.inquiryId(), "DisbursementStatusInquiryResult"),
                "DisbursementStatusInquiryResult", now, request.correlationId(), request.applicationId(),
                request.inquiryId(), request.disbursementCommandId(), result,
                payout == null ? null : payout.getId(),
                payout == null ? null : payout.getAmount(),
                payout == null ? null : payout.getCurrency());
        try {
            outbox.saveAndFlush(new LocalOutboxEvent(event.eventId(), request.applicationId(),
                    LocalKafkaConfiguration.INQUIRY_RESULT, mapper.writeValueAsString(event), now));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize inquiry result", e);
        }
    }
}
