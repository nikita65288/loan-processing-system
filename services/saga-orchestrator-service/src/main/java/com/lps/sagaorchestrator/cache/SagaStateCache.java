package com.lps.sagaorchestrator.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.sagaorchestrator.api.SagaStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Best-effort Redis cache. Never used by Saga business processing. */
@Component
public class SagaStateCache {
    private static final Logger log = LoggerFactory.getLogger(SagaStateCache.class);
    // Avoid flooding WARN logs on every loan while Redis is unavailable.
    private static final long OUTAGE_LOG_INTERVAL_MS = 30_000L;
    private final AtomicLong lastOutageWarningMillis = new AtomicLong();
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final SagaCacheProperties properties;
    private final MeterRegistry metrics;

    public SagaStateCache(StringRedisTemplate redis, ObjectMapper mapper, SagaCacheProperties properties, MeterRegistry metrics) {
        this.redis = redis;
        this.mapper = mapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    /** Records Redis reachability to avoid a second timeout on SET after failed GET. */
    public record Lookup(Optional<SagaStatusResponse> value, boolean available) { }

    public Lookup get(UUID applicationId) {
        Lookup result = getInternal(applicationId);
        String outcome = !properties.enabled() ? "disabled" :
                !result.available() ? "error" :
                (result.value().isPresent() ? "hit" : "miss");
        try { metrics.counter("lps.cache.lookup", "cache", "saga", "outcome", outcome).increment(); }
        catch (RuntimeException ignored) { /* Telemetry never changes business responses. */ }
        return result;
    }

    /** Existing cache-aside policy and failure fallback are unchanged. */
    private Lookup getInternal(UUID applicationId) {
        if (!properties.enabled()) return new Lookup(Optional.empty(), false);
        String json;
        try {
            json = redis.opsForValue().get(keyFor(applicationId));
        } catch (DataAccessException ex) {
            warnUnavailable("GET", applicationId, ex);
            return new Lookup(Optional.empty(), false);
        }
        if (json == null) {
            log.debug("Saga cache MISS applicationId={}", applicationId);
            return new Lookup(Optional.empty(), true);
        }
        try {
            SagaStatusResponse response = mapper.readValue(json, SagaStatusResponse.class);
            if (!valid(response, applicationId)) {
                log.warn("Saga cache invalid snapshot applicationId={}", applicationId);
                evict(applicationId);
                return new Lookup(Optional.empty(), true);
            }
            log.debug("Saga cache HIT applicationId={}", applicationId);
            return new Lookup(Optional.of(response), true);
        } catch (JsonProcessingException ex) {
            log.warn("Saga cache malformed JSON applicationId={}: {}", applicationId, ex.getOriginalMessage());
            evict(applicationId);
            return new Lookup(Optional.empty(), true);
        }
    }

    /** Fixed TTL; failures cannot turn a successful database query into HTTP 500. */
    public void put(SagaStatusResponse response) {
        if (!properties.enabled()) return;
        try {
            redis.opsForValue().set(keyFor(response.applicationId()),
                    mapper.writeValueAsString(response), properties.ttl());
            log.debug("Saga cache SET applicationId={}", response.applicationId());
        } catch (JsonProcessingException ex) {
            log.warn("Saga cache serialization failed applicationId={}: {}",
                    response.applicationId(), ex.getOriginalMessage());
        } catch (DataAccessException ex) {
            warnUnavailable("SET", response.applicationId(), ex);
        }
    }

    /** A failed DEL cannot reverse or invalidate an already committed Saga transition. */
    public void evict(UUID applicationId) {
        if (!properties.enabled()) return;
        try {
            redis.delete(keyFor(applicationId));
            log.debug("Saga cache EVICT applicationId={}", applicationId);
        } catch (DataAccessException ex) {
            warnUnavailable("DEL", applicationId, ex);
        }
    }

    private void warnUnavailable(String action, UUID applicationId, DataAccessException exception) {
        long now = System.currentTimeMillis();
        long previous = lastOutageWarningMillis.get();
        if (now - previous >= OUTAGE_LOG_INTERVAL_MS
                && lastOutageWarningMillis.compareAndSet(previous, now)) {
            log.warn("Saga Redis {} unavailable applicationId={}: {} (further warnings suppressed for 30s)",
                    action, applicationId, exception.toString());
        }
    }

    public String keyFor(UUID applicationId) {
        return properties.keyPrefix() + applicationId;
    }

    private boolean valid(SagaStatusResponse value, UUID applicationId) {
        return value != null && applicationId.equals(value.applicationId())
                && value.sagaId() != null && value.status() != null
                && value.currentStep() != null && value.createdAt() != null
                && value.updatedAt() != null;
    }
}
