package com.lps.sagaorchestrator.saga;

/** Lifecycle status of one durable loan application workflow. */
public enum SagaStatus {
    IN_PROGRESS, COMPLETED, FAILED, REQUIRES_RECONCILIATION
}
