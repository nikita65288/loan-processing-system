package com.lps.loandisbursement.disbursement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.MessageIdentity;
import com.lps.contracts.command.DisburseLoanCommand;
import com.lps.contracts.event.*;
import com.lps.loandisbursement.kafka.LocalKafkaConfiguration;
import com.lps.loandisbursement.outbox.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.Instant;
import java.util.*;

/** Inbox + business record + Outbox are committed atomically, including known invalid commands. */
@Service
public class DisbursementService {
    private static final Logger log=LoggerFactory.getLogger(DisbursementService.class);
    private final DisbursementRepository records;
    private final LocalOutboxRepository outbox;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final MeterRegistry metrics;
    public DisbursementService(DisbursementRepository records,LocalOutboxRepository outbox,
                                JdbcTemplate jdbc,ObjectMapper mapper,MeterRegistry metrics) {
        this.records=records;this.outbox=outbox;this.jdbc=jdbc;this.mapper=mapper;this.metrics=metrics;
    }
    @Transactional
    public void process(DisburseLoanCommand cmd) {
        Objects.requireNonNull(cmd,"Missing command");
        Objects.requireNonNull(cmd.commandId(),"Missing commandId");
        Objects.requireNonNull(cmd.applicationId(),"Missing applicationId");
        Objects.requireNonNull(cmd.correlationId(),"Missing correlationId");
        String fingerprint=String.valueOf(cmd.amount()==null?null:cmd.amount().stripTrailingZeros())
                +":"+cmd.currency()+":"+cmd.correlationId()+":"+cmd.applicationId();
        if (jdbc.update("""
                INSERT INTO loan_disbursement.processed_commands
                (command_id,application_id,request_fingerprint,outcome,created_at)
                VALUES (?,?,?,'PROCESSED',now()) ON CONFLICT (command_id) DO NOTHING
                """,cmd.commandId(),cmd.applicationId(),fingerprint)==0) {
            String saved=jdbc.queryForObject("""
                    SELECT request_fingerprint FROM loan_disbursement.processed_commands WHERE command_id=?
                    """,String.class,cmd.commandId());
            if(!fingerprint.equals(saved)) throw new IllegalStateException("Conflicting duplicate payout command");
            return;
        }
        Instant now=Instant.now();
        if (cmd.amount()==null || cmd.amount().signum()<=0 || cmd.amount().scale()>2
                || cmd.amount().precision()>19 || cmd.currency()==null
                || !cmd.currency().matches("[A-Z]{3}")) {
            LoanDisbursementFailedEvent failed=new LoanDisbursementFailedEvent(
                    MessageIdentity.resultEventId(cmd.commandId(),"LoanDisbursementFailed"),
                    "LoanDisbursementFailed",now,cmd.correlationId(),cmd.applicationId(),
                    cmd.commandId(),"INVALID_DISBURSEMENT_INPUT",false);
            storeEvent(cmd.applicationId(),LocalKafkaConfiguration.FAILED,failed.eventId(),failed,now);
            return;
        }
        // The DB UNIQUE constraint on application_id is authoritative, not this SELECT.
        var existing=records.findByApplicationId(cmd.applicationId());
        if (existing.isPresent()) {
            var payout=existing.get();
            if (payout.getAmount().compareTo(cmd.amount())!=0 || !payout.getCurrency().equals(cmd.currency())) {
                // Durable conflict diagnostics, without changing the original payment/result.
                jdbc.update("UPDATE loan_disbursement.processed_commands SET outcome='CONFLICT' WHERE command_id=?",
                        cmd.commandId());
                log.error("Conflicting simulated payout request applicationId={} commandId={}",
                        cmd.applicationId(),cmd.commandId());
            }
            return;
        }
        UUID payoutId=UUID.randomUUID();
        records.saveAndFlush(new DisbursementRecord(payoutId,cmd.applicationId(),cmd.commandId(),
                cmd.amount(),cmd.currency(),now));
        LoanDisbursementCompletedEvent completed=new LoanDisbursementCompletedEvent(
                MessageIdentity.resultEventId(cmd.commandId(),"LoanDisbursementCompleted"),
                "LoanDisbursementCompleted",now,cmd.correlationId(),cmd.applicationId(),
                cmd.commandId(),payoutId,cmd.amount(),cmd.currency());
        storeEvent(cmd.applicationId(),LocalKafkaConfiguration.COMPLETED,completed.eventId(),completed,now);
    }
    private void storeEvent(UUID applicationId,String topic,UUID eventId,Object event,Instant now) {
        try {
            outbox.saveAndFlush(new LocalOutboxEvent(eventId,applicationId,topic,
                    mapper.writeValueAsString(event),now));
            outcomeMetric(topic);
        } catch(JsonProcessingException e) {throw new IllegalStateException("Cannot serialize payout result",e);}
    }

    /** Count the simulated payout result, not real money transferred. */
    private void outcomeMetric(String topic) {
        String result = LocalKafkaConfiguration.COMPLETED.equals(topic) ? "simulated_success" : "failed";
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() {
                    try { metrics.counter("lps.disbursement.outcomes", "result", result).increment(); }
                    catch (RuntimeException ignored) { /* Metrics must not affect money-state correctness. */ }
                }
            });
        }
    }
}
