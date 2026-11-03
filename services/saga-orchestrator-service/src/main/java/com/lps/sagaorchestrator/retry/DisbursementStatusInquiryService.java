package com.lps.sagaorchestrator.retry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.contracts.command.DisbursementStatusInquiryCommand;
import com.lps.sagaorchestrator.kafka.SagaKafkaConfiguration;
import com.lps.sagaorchestrator.outbox.SagaOutboxEvent;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.saga.SagaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Durable bounded inquiry attempts, sharing the existing Saga timeout scheduler and Saga row lock. */
@Service
public class DisbursementStatusInquiryService {
    private static final Logger log = LoggerFactory.getLogger(DisbursementStatusInquiryService.class);
    private final SagaRepository sagas;
    private final SagaStepAttemptRepository steps;
    private final SagaOutboxRepository outbox;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final boolean enabled;
    private final int maxAttempts;
    private final Duration retryInterval;
    private final int batchSize;
    private final TransactionTemplate transactions;

    public DisbursementStatusInquiryService(SagaRepository sagas, SagaStepAttemptRepository steps,
            SagaOutboxRepository outbox, JdbcTemplate jdbc, ObjectMapper mapper, Clock clock,
            PlatformTransactionManager transactionManager,
            @Value("${lps.saga.inquiry.enabled:true}") boolean enabled,
            @Value("${lps.saga.inquiry.max-attempts:5}") int maxAttempts,
            @Value("${lps.saga.inquiry.retry-interval:30s}") Duration retryInterval,
            @Value("${lps.saga.inquiry.batch-size:50}") int batchSize) {
        if (maxAttempts < 1 || retryInterval.isNegative() || retryInterval.isZero() || batchSize < 1)
            throw new IllegalArgumentException("Invalid status inquiry retry configuration");
        this.sagas = sagas;
        this.steps = steps;
        this.outbox = outbox;
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.clock = clock;
        this.enabled = enabled;
        this.maxAttempts = maxAttempts;
        this.retryInterval = retryInterval;
        this.batchSize = batchSize;
        this.transactions = new TransactionTemplate(transactionManager);
    }

    /** Read-only bounded scan: a PUBLISHED previous inquiry is required before a new attempt. */
    public void scan() {
        if (!enabled) return;
        Instant cutoff = clock.instant().minus(retryInterval);
        List<UUID> due = jdbc.queryForList("""
                SELECT s.saga_id FROM saga_orchestrator.saga_instances s
                LEFT JOIN LATERAL (
                   SELECT a.attempt_number, a.inquiry_id, a.created_at
                   FROM saga_orchestrator.disbursement_inquiry_attempts a
                   WHERE a.saga_id = s.saga_id ORDER BY a.attempt_number DESC LIMIT 1
                ) last ON true
                LEFT JOIN saga_orchestrator.saga_outbox_events o ON o.id = last.inquiry_id
                WHERE s.status = 'REQUIRES_RECONCILIATION'
                   AND s.current_step = 'DISBURSEMENT'
                   AND s.failure_reason = 'DISBURSEMENT_UNKNOWN'
                   AND s.disbursement_status = 'UNKNOWN' AND s.disbursement_id IS NULL
                   AND s.final_decision = 'APPROVED' AND s.correlation_id IS NOT NULL
                   AND (SELECT count(DISTINCT step.command_id)
                        FROM saga_orchestrator.saga_step_attempts step
                        WHERE step.saga_id=s.saga_id AND step.step='DISBURSEMENT') = 1
                   AND COALESCE(last.attempt_number, 0) < ?
                   AND (last.inquiry_id IS NULL OR (last.created_at <= ? AND o.status = 'PUBLISHED'))
                ORDER BY s.failed_at, s.saga_id LIMIT ?
                """, UUID.class, maxAttempts, Timestamp.from(cutoff), batchSize);
        for (UUID sagaId : due) {
            // Call through TransactionTemplate: self-invocation would bypass @Transactional proxy.
            try { transactions.executeWithoutResult(tx -> initiate(sagaId)); }
            catch (Exception e) { log.error("Disbursement inquiry initiation failed sagaId={}", sagaId, e); }
        }
    }

    /** Saga lock serializes schedulers and completion listeners across any number of replicas. */
    @Transactional
    public void initiate(UUID sagaId) {
        if (!enabled) return;
        var optional = sagas.findByIdForUpdate(sagaId);
        if (optional.isEmpty()) return;
        var saga = optional.get();
        if (!saga.awaitsLateDisbursementConfirmation()) return;
        if (saga.getCorrelationId() == null) {
            log.warn("Cannot inquire: missing Saga business correlation sagaId={}", sagaId);
            return;
        }
        var payoutAttempt = steps.findFirstBySagaIdAndStepOrderByAttemptNumberDesc(
                sagaId, com.lps.sagaorchestrator.saga.SagaStep.DISBURSEMENT);
        if (payoutAttempt.isEmpty()) {
            log.warn("Cannot inquire: missing original payout identity sagaId={}", sagaId);
            return;
        }
        // An ambiguous historical payout identity must not be silently resolved as a new operation.
        Long distinctOperations = jdbc.queryForObject("""
                SELECT count(DISTINCT command_id) FROM saga_orchestrator.saga_step_attempts
                WHERE saga_id=? AND step='DISBURSEMENT'
                """, Long.class, sagaId);
        if (distinctOperations == null || distinctOperations != 1) {
            log.error("Cannot inquire: ambiguous historical payout identity sagaId={}", sagaId);
            return;
        }
        UUID commandId = payoutAttempt.get().getCommandId();
        var last = jdbc.query("""
                SELECT a.attempt_number, a.created_at, o.status
                FROM saga_orchestrator.disbursement_inquiry_attempts a
                JOIN saga_orchestrator.saga_outbox_events o ON o.id=a.inquiry_id
                WHERE a.saga_id=? ORDER BY a.attempt_number DESC LIMIT 1
                """, (rs, i) -> new PriorAttempt(rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getString(3)), sagaId);
        Instant now = clock.instant();
        int attemptNumber = 1;
        if (!last.isEmpty()) {
            var prev = last.get(0);
            if (prev.number() >= maxAttempts || !"PUBLISHED".equals(prev.outboxStatus())
                    || now.isBefore(prev.createdAt().plus(retryInterval))) return;
            attemptNumber = prev.number() + 1;
        }
        UUID inquiryId = UUID.randomUUID();
        var request = new DisbursementStatusInquiryCommand(inquiryId, "DisbursementStatusInquiry", now,
                saga.getCorrelationId(), saga.getApplicationId(), commandId);
        try {
            outbox.saveAndFlush(new SagaOutboxEvent(inquiryId, sagaId, saga.getApplicationId(),
                    request.commandType(), SagaKafkaConfiguration.DISBURSEMENT_INQUIRY_REQUESTED_TOPIC,
                    mapper.writeValueAsString(request), now));
            jdbc.update("""
                    INSERT INTO saga_orchestrator.disbursement_inquiry_attempts
                      (inquiry_id, saga_id, disbursement_command_id, attempt_number, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """, inquiryId, sagaId, commandId, attemptNumber, Timestamp.from(now));
            log.info("Disbursement status inquiry persisted applicationId={} payoutCommandId={} inquiryId={} attempt={}",
                    saga.getApplicationId(), commandId, inquiryId, attemptNumber);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize inquiry request", e);
        }
    }

    private record PriorAttempt(int number, Instant createdAt, String outboxStatus) {}
}
