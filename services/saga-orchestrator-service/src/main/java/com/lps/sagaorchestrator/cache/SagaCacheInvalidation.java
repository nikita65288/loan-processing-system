package com.lps.sagaorchestrator.cache;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

/** Schedules optional eviction ONLY after a successful PostgreSQL transaction commit. */
@Component
public class SagaCacheInvalidation {
    private final SagaStateCache cache;

    public SagaCacheInvalidation(SagaStateCache cache) {
        this.cache = cache;
    }

    /** Must be called inside the same transaction that changes a Saga snapshot field. */
    public void afterCommit(UUID applicationId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("Saga cache invalidation must be registered in a transaction");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                cache.evict(applicationId);
            }
        });
    }
}
