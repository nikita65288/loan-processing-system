package com.lps.sagaorchestrator.api;

import com.lps.sagaorchestrator.cache.SagaStateCache;
import com.lps.sagaorchestrator.saga.SagaRepository;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.util.UUID;

/** Read-only query path; no Redis value is used by listeners or retry schedulers. */
@Service
public class SagaQueryService {
    private final SagaRepository sagas;
    private final SagaStateCache cache;
    private final MeterRegistry metrics;

    public SagaQueryService(SagaRepository sagas, SagaStateCache cache, MeterRegistry metrics) {
        this.sagas = sagas;
        this.cache = cache;
        this.metrics = metrics;
    }

    /** Cache HIT may be stale; cache MISS or outage reads committed PostgreSQL state. */
    public SagaStatusResponse getByApplicationId(UUID applicationId) {
        var lookup = cache.get(applicationId);
        if (lookup.value().isPresent()) return lookup.value().get();
        var saga = sagas.findByApplicationId(applicationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Saga not found"));
        var response = SagaStatusResponse.from(saga);
        if (!lookup.available()) {
            // A fallback is successful only once committed PostgreSQL state is read.
            try { metrics.counter("lps.cache.fallback", "cache", "saga").increment(); }
            catch (RuntimeException ignored) { /* Telemetry cannot change business responses. */ }
        }
        if (lookup.available()) cache.put(response);
        return response;
    }
}
