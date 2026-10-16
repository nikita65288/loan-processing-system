package com.lps.sagaorchestrator.recovery;

import com.lps.sagaorchestrator.outbox.SagaOutboxDiagnosticRow;
import com.lps.sagaorchestrator.outbox.SagaOutboxRepository;
import com.lps.sagaorchestrator.outbox.SagaOutboxStatus;
import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaRepository;
import com.lps.sagaorchestrator.saga.SagaStatus;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Reads persisted Saga/Outbox facts; never issues business commands or changes DB state. */
@Service
public class SagaRecoveryService {
    private static final Logger log = LoggerFactory.getLogger(SagaRecoveryService.class);
    private static final int MAX_DIAGNOSTIC_EXAMPLES = 10;

    private final SagaRepository sagas;
    private final SagaOutboxRepository outbox;
    private final int batchSize;
    private final int maxSagas;

    public SagaRecoveryService(SagaRepository sagas, SagaOutboxRepository outbox,
                               @Value("${lps.recovery.batch-size:100}") int batchSize,
                               @Value("${lps.recovery.max-sagas:1000}") int maxSagas) {
        if (batchSize < 1 || maxSagas < 1) {
            throw new IllegalArgumentException("Recovery scan limits must be positive");
        }
        this.sagas = sagas;
        this.outbox = outbox;
        this.batchSize = batchSize;
        this.maxSagas = maxSagas;
    }

    /** Each repository query is short-lived; a concurrent transition may make diagnostics stale. */
    public SagaRecoverySummary inspect() {
        var counts = new EnumMap<SagaRecoveryClassification, Long>(SagaRecoveryClassification.class);
        long inspected = 0;
        int page = 0;
        int examples = 0;
        boolean more = false;
        do {
            // Stable ordering avoids arbitrary page boundaries; no long-lived DB transaction.
            Slice<SagaInstance> slice = sagas.findByStatus(SagaStatus.IN_PROGRESS,
                    PageRequest.of(page++, batchSize, Sort.by("sagaId")));
            List<SagaInstance> batch = slice.getContent();
            if (batch.isEmpty()) break;

            var applicationIds = batch.stream().map(SagaInstance::getApplicationId).toList();
            Map<UUID, List<SagaOutboxDiagnosticRow>> commands = outbox
                    .findDiagnosticRows(applicationIds).stream()
                    .collect(Collectors.groupingBy(SagaOutboxDiagnosticRow::getApplicationId));

            int processedInBatch = 0;
            for (SagaInstance saga : batch) {
                if (inspected == maxSagas) break;
                SagaRecoveryClassification classification = classify(saga.getCurrentStep(),
                        commands.getOrDefault(saga.getApplicationId(), List.of()));
                counts.merge(classification, 1L, Long::sum);
                inspected++;
                processedInBatch++;
                if (examples < MAX_DIAGNOSTIC_EXAMPLES
                        && (classification == SagaRecoveryClassification.INCONSISTENT
                        || classification == SagaRecoveryClassification.PUBLICATION_FAILED)) {
                    log.warn("Saga recovery inspection applicationId={} sagaId={} step={} state={}",
                            saga.getApplicationId(), saga.getSagaId(), saga.getCurrentStep(), classification);
                    examples++;
                }
            }
            more = slice.hasNext() || processedInBatch < batch.size();
        } while (more && inspected < maxSagas);
        // This is an operational snapshot, not a consistent snapshot across concurrent commits.
        return new SagaRecoverySummary(inspected, counts, more && inspected >= maxSagas);
    }

    /** A missing or ambiguous command is reported, never regenerated. */
    static SagaRecoveryClassification classify(SagaStep step, List<SagaOutboxDiagnosticRow> outboxRows) {
        String expectedType = switch (step) {
            case SCORING -> "PerformCreditScoring";
            case BUREAU -> "PerformBureauCheck";
            case DECISION -> "MakeDecision";
            case DISBURSEMENT -> "DisburseLoan";
            case NOTIFICATION -> "SendLoanDecisionNotification";
            case FINISHED -> null;
        };
        if (expectedType == null) return SagaRecoveryClassification.INCONSISTENT;
        List<SagaOutboxDiagnosticRow> matching = outboxRows.stream()
                .filter(row -> expectedType.equals(row.getCommandType())).toList();
        if (matching.isEmpty()) return SagaRecoveryClassification.INCONSISTENT;
        // Multiple commands are normal after business retries. Only use ordering if unambiguous.
        SagaOutboxDiagnosticRow latest = matching.get(0);
        if (matching.size() > 1) {
            if (matching.stream().anyMatch(row -> row.getCreatedAt() == null)
                    || matching.stream().map(SagaOutboxDiagnosticRow::getCreatedAt).distinct().count()
                    != matching.size()) return SagaRecoveryClassification.INCONSISTENT;
            latest = matching.stream().max(Comparator.comparing(
                    SagaOutboxDiagnosticRow::getCreatedAt)).orElseThrow();
        }
        SagaOutboxStatus status = latest.getStatus();
        if (status == null) return SagaRecoveryClassification.INCONSISTENT;
        return switch (status) {
            case NEW -> SagaRecoveryClassification.WAITING_FOR_PUBLICATION;
            case CLAIMED -> SagaRecoveryClassification.PUBLICATION_IN_PROGRESS;
            case PUBLISHED -> SagaRecoveryClassification.AWAITING_COMPLETION;
            case FAILED -> SagaRecoveryClassification.PUBLICATION_FAILED;
        };
    }
}
