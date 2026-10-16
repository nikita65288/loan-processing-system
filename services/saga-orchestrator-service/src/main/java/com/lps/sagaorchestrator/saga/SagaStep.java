package com.lps.sagaorchestrator.saga;

/** SCORING means the command was scheduled, not that scoring has finished. */
public enum SagaStep {
    SCORING, BUREAU, DECISION, DISBURSEMENT, NOTIFICATION, FINISHED
}
