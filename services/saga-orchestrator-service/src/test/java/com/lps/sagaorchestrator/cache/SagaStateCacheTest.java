package com.lps.sagaorchestrator.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lps.contracts.model.Decision;
import com.lps.sagaorchestrator.api.SagaStatusResponse;
import com.lps.sagaorchestrator.saga.SagaStatus;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Serialization, key namespace, TTL, disabled mode and Redis failure isolation. */
class SagaStateCacheTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final SagaCacheProperties enabled = new SagaCacheProperties(true, Duration.ofSeconds(30), "lps:saga:v1:");
    private SagaStateCache cache;
    private SagaStatusResponse response;

    @BeforeEach
    void setUp() {
        cache = new SagaStateCache(redis, mapper, enabled, new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        when(redis.opsForValue()).thenReturn(values);
        response = new SagaStatusResponse(UUID.randomUUID(), UUID.randomUUID(), SagaStatus.COMPLETED,
                SagaStep.FINISHED, Decision.APPROVED, "COMPLETED", "SENT_SIMULATED", null,
                Instant.parse("2026-10-12T09:00:00Z"), Instant.parse("2026-10-12T09:00:02Z"),
                Instant.parse("2026-10-12T09:00:02Z"));
    }

    @Test
    void keyAndTtlAndSerializationRoundTrip() throws Exception {
        var id = response.applicationId();
        var key = "lps:saga:v1:" + id;
        assertEquals(key, cache.keyFor(id));
        cache.put(response);
        verify(values).set(eq(key), anyString(), eq(Duration.ofSeconds(30)));
        String json = mapper.writeValueAsString(response);
        when(values.get(key)).thenReturn(json);
        assertEquals(response, cache.get(id).value().orElseThrow());
    }

    @Test
    void missAndRedisGetFailureAreDifferent() {
        var id = response.applicationId();
        assertTrue(cache.get(id).available());
        when(values.get(cache.keyFor(id))).thenThrow(new RedisConnectionFailureException("down"));
        var lookup = cache.get(id);
        assertFalse(lookup.available());
        assertTrue(lookup.value().isEmpty());
    }

    @Test
    void malformedOrMismatchedJsonEvictsAndFallsBack() throws Exception {
        var id = response.applicationId();
        when(values.get(cache.keyFor(id))).thenReturn("{bad json", "{\"sagaId\":\"" + UUID.randomUUID() + "\"}");
        assertTrue(cache.get(id).value().isEmpty());
        assertTrue(cache.get(id).value().isEmpty());
        verify(redis, times(2)).delete(cache.keyFor(id));
    }

    @Test
    void redisSetOrDeleteFailureDoesNotPropagate() {
        var id = response.applicationId();
        doThrow(new RedisConnectionFailureException("down"))
                .when(values).set(anyString(), anyString(), any(Duration.class));
        when(redis.delete(cache.keyFor(id))).thenThrow(new RedisConnectionFailureException("down"));
        assertDoesNotThrow(() -> cache.put(response));
        assertDoesNotThrow(() -> cache.evict(id));
    }

    @Test
    void disabledCacheNeverTouchesRedis() {
        var disabled = new SagaStateCache(redis, mapper,
                new SagaCacheProperties(false, Duration.ofSeconds(30), "lps:saga:v1:"), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        clearInvocations(redis);
        assertFalse(disabled.get(response.applicationId()).available());
        disabled.put(response);
        disabled.evict(response.applicationId());
        verifyNoInteractions(redis);
    }
}
