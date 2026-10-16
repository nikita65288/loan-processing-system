package com.lps.sagaorchestrator.recovery;

import java.util.Map;

/** Immutable aggregate of a bounded, best-effort startup inspection. */
public record SagaRecoverySummary(
        long inspected,
        Map<SagaRecoveryClassification, Long> counts,
        boolean truncated
) {
    public SagaRecoverySummary {
        counts = Map.copyOf(counts);
    }

    public long count(SagaRecoveryClassification classification) {
        return counts.getOrDefault(classification, 0L);
    }
}
