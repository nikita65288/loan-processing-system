package com.lps.sagaorchestrator.outbox;

/** Publication lifecycle, independent of the business Saga state. */
public enum SagaOutboxStatus {
    NEW, CLAIMED, PUBLISHED, FAILED
}
