package com.lps.sagaorchestrator.recovery;

/** Operational observations, not Saga business states or retry instructions. */
public enum SagaRecoveryClassification {
    WAITING_FOR_PUBLICATION,
    PUBLICATION_IN_PROGRESS,
    PUBLICATION_FAILED,
    AWAITING_COMPLETION,
    INCONSISTENT
}
