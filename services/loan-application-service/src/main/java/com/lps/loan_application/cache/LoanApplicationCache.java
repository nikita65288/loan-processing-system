package com.lps.loan_application.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lps.loan_application.dto.LoanApplicationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.UUID;

/**
 * Optional Redis read cache. It stores only the public response representation,
 * never a JPA entity or durable business state. Redis errors are best-effort.
 */
@Component
public class LoanApplicationCache {

    private static final Logger log = LoggerFactory.getLogger(LoanApplicationCache.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final LoanApplicationCacheProperties properties;
    private final MeterRegistry metrics;

    public LoanApplicationCache(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            LoanApplicationCacheProperties properties,
            MeterRegistry metrics
    ) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Redis availability is kept separate from an ordinary MISS, so the caller
     * can skip a second Redis timeout (SET) when the preceding GET already failed.
     */
    public record Lookup(Optional<LoanApplicationResponse> response, boolean available) {}

    public Lookup get(UUID applicationId) {
        Lookup result = getInternal(applicationId);
        String outcome = !properties.enabled() ? "disabled" :
                !result.available() ? "error" :
                (result.response().isPresent() ? "hit" : "miss");
        try { metrics.counter("lps.cache.lookup", "cache", "loan_application", "outcome", outcome).increment(); }
        catch (RuntimeException ignored) { /* Telemetry never changes business responses. */ }
        return result;
    }

    /** Existing cache-aside policy and failure fallback are unchanged. */
    private Lookup getInternal(UUID applicationId) {
        if (!properties.enabled()) {
            return new Lookup(Optional.empty(), false);
        }

        String key = keyFor(applicationId);
        String json;
        try {
            json = redisTemplate.opsForValue().get(key);
        } catch (DataAccessException exception) {
            log.warn("Redis cache read unavailable for application {}: {}",
                    applicationId, exception.toString());
            return new Lookup(Optional.empty(), false);
        }

        if (json == null) {
            log.debug("Loan application cache MISS for {}", applicationId);
            return new Lookup(Optional.empty(), true);
        }

        try {
            LoanApplicationResponse response =
                    objectMapper.readValue(json, LoanApplicationResponse.class);
            if (!isValid(response, applicationId)) {
                log.warn("Invalid loan application cache value for {}", applicationId);
                evict(applicationId);
                return new Lookup(Optional.empty(), true);
            }
            log.debug("Loan application cache HIT for {}", applicationId);
            return new Lookup(Optional.of(response), true);
        } catch (JsonProcessingException exception) {
            log.warn("Cannot deserialize loan application cache value for {}: {}",
                    applicationId, exception.getOriginalMessage());
            evict(applicationId);
            return new Lookup(Optional.empty(), true);
        }
    }

    /** Cache population must not change the outcome of a successful DB read. */
    public void put(LoanApplicationResponse response) {
        if (!properties.enabled()) {
            return;
        }

        try {
            String json = objectMapper.writeValueAsString(response);
            redisTemplate.opsForValue().set(keyFor(response.id()), json, properties.ttl());
            log.debug("Loan application cache populated for {}", response.id());
        } catch (JsonProcessingException exception) {
            log.warn("Cannot serialize loan application response for cache {}: {}",
                    response.id(), exception.getOriginalMessage());
        } catch (DataAccessException exception) {
            log.warn("Redis cache write unavailable for application {}: {}",
                    response.id(), exception.toString());
        }
    }

    /** Reserved for a future post-commit invalidation when updates are introduced. */
    public void evict(UUID applicationId) {
        if (!properties.enabled()) {
            return;
        }
        try {
            redisTemplate.delete(keyFor(applicationId));
            log.debug("Loan application cache evicted for {}", applicationId);
        } catch (DataAccessException exception) {
            log.warn("Redis cache eviction unavailable for application {}: {}",
                    applicationId, exception.toString());
        }
    }

    public String keyFor(UUID applicationId) {
        return properties.keyPrefix() + applicationId;
    }

    /** Reject partial, mismatched or incompatible payloads instead of returning fake data. */
    private boolean isValid(LoanApplicationResponse response, UUID applicationId) {
        return response != null
                && applicationId.equals(response.id())
                && response.customerId() != null
                && response.amount() != null
                && response.term() != null
                && response.currency() != null
                && response.status() != null
                && response.createdAt() != null
                && response.updatedAt() != null;
    }
}
