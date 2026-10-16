package com.lps.sagaorchestrator.api;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.cache.SagaStateCache;
import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Query unit tests prove Cache-Aside read behavior independently of the broker. */
class SagaQueryServiceTest {
    private SagaRepository sagas;
    private SagaStateCache cache;
    private SagaQueryService query;
    private UUID applicationId;
    private SagaInstance saga;

    @BeforeEach
    void setUp() {
        sagas = mock(SagaRepository.class);
        cache = mock(SagaStateCache.class);
        query = new SagaQueryService(sagas, cache, new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        applicationId = UUID.randomUUID();
        saga = SagaInstance.start(new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated",
                Instant.now(), UUID.randomUUID(), applicationId, UUID.randomUUID(),
                new BigDecimal("5000.00"), 12, "EUR"), Instant.now());
    }

    @Test
    void hitSkipsDatabase() {
        var response = SagaStatusResponse.from(saga);
        when(cache.get(applicationId)).thenReturn(new SagaStateCache.Lookup(Optional.of(response), true));
        assertSame(response, query.getByApplicationId(applicationId));
        verifyNoInteractions(sagas);
        verify(cache, never()).put(any());
    }

    @Test
    void missReadsDatabaseAndPopulatesCache() {
        when(cache.get(applicationId)).thenReturn(new SagaStateCache.Lookup(Optional.empty(), true));
        when(sagas.findByApplicationId(applicationId)).thenReturn(Optional.of(saga));
        var result = query.getByApplicationId(applicationId);
        assertEquals(applicationId, result.applicationId());
        assertEquals(saga.getStatus(), result.status());
        verify(cache).put(result);
    }

    @Test
    void absentSagaReturns404WithoutNegativeCache() {
        when(cache.get(applicationId)).thenReturn(new SagaStateCache.Lookup(Optional.empty(), true));
        when(sagas.findByApplicationId(applicationId)).thenReturn(Optional.empty());
        assertEquals(404, assertThrows(ResponseStatusException.class,
                () -> query.getByApplicationId(applicationId)).getStatusCode().value());
        verify(cache, never()).put(any());
    }

    @Test
    void redisFailureFallsBackWithoutSecondRedisCall() {
        when(cache.get(applicationId)).thenReturn(new SagaStateCache.Lookup(Optional.empty(), false));
        when(sagas.findByApplicationId(applicationId)).thenReturn(Optional.of(saga));
        assertEquals(applicationId, query.getByApplicationId(applicationId).applicationId());
        verify(cache, never()).put(any());
    }
}
