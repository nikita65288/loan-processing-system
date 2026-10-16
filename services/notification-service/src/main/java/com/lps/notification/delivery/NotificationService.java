package com.lps.notification.delivery;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.MessageIdentity;
import com.lps.contracts.command.SendLoanDecisionNotificationCommand;
import com.lps.contracts.event.*;
import com.lps.contracts.model.*;
import com.lps.notification.kafka.LocalKafkaConfiguration;
import com.lps.notification.outbox.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.Instant;
import java.util.*;

/** Persisted stub: inbox, simulated delivery and result Outbox share one transaction. */
@Service
public class NotificationService {
    private final NotificationRepository deliveries;
    private final JdbcTemplate jdbc;
    private final LocalOutboxRepository outbox;
    private final ObjectMapper mapper;
    private final MeterRegistry metrics;
    public NotificationService(NotificationRepository deliveries,JdbcTemplate jdbc,
                               LocalOutboxRepository outbox,ObjectMapper mapper,MeterRegistry metrics) {
        this.deliveries=deliveries;this.jdbc=jdbc;this.outbox=outbox;this.mapper=mapper;this.metrics=metrics;
    }
    @Transactional
    public void process(SendLoanDecisionNotificationCommand cmd) {
        Objects.requireNonNull(cmd,"Missing notification command");
        Objects.requireNonNull(cmd.commandId(),"Missing commandId");
        Objects.requireNonNull(cmd.applicationId(),"Missing applicationId");
        String fingerprint=cmd.applicationId()+":"+cmd.customerId()+":"+cmd.finalDecision()+":"
                +cmd.notificationType()+":"+cmd.correlationId();
        if(jdbc.update("""
                INSERT INTO notification.processed_commands(command_id,application_id,request_fingerprint,created_at)
                VALUES(?,?,?,now()) ON CONFLICT(command_id) DO NOTHING
                """,cmd.commandId(),cmd.applicationId(),fingerprint)==0) {
            String previous=jdbc.queryForObject("""
                    SELECT request_fingerprint FROM notification.processed_commands WHERE command_id=?
                    """,String.class,cmd.commandId());
            if(!fingerprint.equals(previous)) throw new IllegalStateException("Conflicting notification command identity");
            return;
        }
        Instant now=Instant.now();
        boolean valid=cmd.customerId()!=null && cmd.correlationId()!=null &&
                ((cmd.finalDecision()==Decision.APPROVED && cmd.notificationType()==NotificationType.LOAN_APPROVED_AND_DISBURSED)
                ||(cmd.finalDecision()==Decision.REJECTED && cmd.notificationType()==NotificationType.LOAN_REJECTED));
        if(!valid) {
            LoanNotificationFailedEvent failed=new LoanNotificationFailedEvent(
                    MessageIdentity.resultEventId(cmd.commandId(),"LoanNotificationFailed"),
                    "LoanNotificationFailed",now,cmd.correlationId(),cmd.applicationId(),
                    cmd.commandId(),"INVALID_NOTIFICATION_INPUT",false);
            store(cmd.applicationId(),LocalKafkaConfiguration.FAILED,failed.eventId(),failed,now);
            return;
        }
        // Same application and different notification type is a durable protocol conflict.
        var previous=deliveries.findByApplicationId(cmd.applicationId());
        if(previous.isPresent()) {
            if(previous.get().getNotificationType()!=cmd.notificationType())
                throw new IllegalStateException("Conflicting notification type for "+cmd.applicationId());
            return;
        }
        deliveries.saveAndFlush(new NotificationDelivery(UUID.randomUUID(),cmd.applicationId(),
                cmd.commandId(),cmd.customerId(),cmd.notificationType(),now));
        LoanNotificationCompletedEvent completed=new LoanNotificationCompletedEvent(
                MessageIdentity.resultEventId(cmd.commandId(),"LoanNotificationCompleted"),
                "LoanNotificationCompleted",now,cmd.correlationId(),cmd.applicationId(),
                cmd.commandId(),cmd.notificationType());
        store(cmd.applicationId(),LocalKafkaConfiguration.COMPLETED,completed.eventId(),completed,now);
    }
    private void store(UUID applicationId,String topic,UUID id,Object payload,Instant now) {
        try {outbox.saveAndFlush(new LocalOutboxEvent(id,applicationId,topic,
                mapper.writeValueAsString(payload),now));
            outcomeMetric(topic);
        }
        catch(JsonProcessingException e){throw new IllegalStateException("Cannot serialize notification result",e);}
    }

    /** Counts durable simulated notification results only after the transaction commits. */
    private void outcomeMetric(String topic) {
        String result = LocalKafkaConfiguration.COMPLETED.equals(topic) ? "simulated" : "failed";
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() {
                    try { metrics.counter("lps.notification.outcomes", "result", result).increment(); }
                    catch (RuntimeException ignored) { /* Logs/metrics are best effort. */ }
                }
            });
        }
    }
}
