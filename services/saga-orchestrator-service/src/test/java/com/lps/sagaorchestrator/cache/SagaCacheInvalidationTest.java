package com.lps.sagaorchestrator.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises Spring transaction callbacks without requiring Redis or PostgreSQL. */
class SagaCacheInvalidationTest {
    private final SagaStateCache cache = mock(SagaStateCache.class);
    private final SagaCacheInvalidation invalidation = new SagaCacheInvalidation(cache);

    @AfterEach
    void clean() {
        if (TransactionSynchronizationManager.isSynchronizationActive())
            TransactionSynchronizationManager.clearSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void evictsOnlyAfterCommit() {
        UUID id = UUID.randomUUID();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        invalidation.afterCommit(id);
        verifyNoInteractions(cache);
        TransactionSynchronizationManager.getSynchronizations().forEach(sync -> sync.afterCommit());
        verify(cache).evict(id);
    }

    @Test
    void rollbackDoesNotEvict() {
        UUID id = UUID.randomUUID();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        invalidation.afterCommit(id);
        // TransactionManager does not run afterCommit() on rollback.
        TransactionSynchronizationManager.getSynchronizations().forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
        verifyNoInteractions(cache);
    }

    @Test
    void noTransactionFailsFast() {
        assertThrows(IllegalStateException.class, () -> invalidation.afterCommit(UUID.randomUUID()));
    }
}
