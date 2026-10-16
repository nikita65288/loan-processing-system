package com.lps.loan_application.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.entity.LoanApplicationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LoanApplicationCacheTest {

    private static final String PREFIX = "lps:loan-application:v1:";
    private static final Duration TTL = Duration.ofMinutes(5);

    @Mock private StringRedisTemplate redis;
    @Mock private ValueOperations<String, String> values;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private LoanApplicationCache cache;

    @BeforeEach
    void setUp() {
        cache = new LoanApplicationCache(redis, mapper,
                new LoanApplicationCacheProperties(true, TTL, PREFIX), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    @Test
    void shouldGenerateDeterministicNamespacedKey() {
        UUID id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        assertEquals(PREFIX + id, cache.keyFor(id));
    }

    @Test
    void shouldReturnMissWhenKeyAbsent() {
        var response = response();
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(PREFIX + response.id())).thenReturn(null);
        assertEquals(Optional.empty(), cache.get(response.id()).response());
    }

    @Test
    void shouldRoundTripJsonWithoutLosingFields() throws Exception {
        var expected = response();
        when(redis.opsForValue()).thenReturn(values);
        cache.put(expected);

        var jsonCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(values).set(eq(PREFIX + expected.id()), jsonCaptor.capture(), eq(TTL));
        String json = jsonCaptor.getValue();
        assertTrue(json.contains("123456789.12"));
        assertTrue(json.contains("2026-10-12T10:15:30Z"));
        assertFalse(json.contains("@class"));

        when(values.get(PREFIX + expected.id())).thenReturn(json);
        assertEquals(Optional.of(expected), cache.get(expected.id()).response());
    }

    @Test
    void shouldFallBackOnRedisGetFailure() {
        var response = response();
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(anyString())).thenThrow(new RedisConnectionFailureException("down"));
        var lookup = cache.get(response.id());
        assertEquals(Optional.empty(), lookup.response());
        assertFalse(lookup.available());
    }

    @Test
    void shouldIgnoreRedisSetFailure() {
        var response = response();
        when(redis.opsForValue()).thenReturn(values);
        doThrow(new RedisConnectionFailureException("down"))
                .when(values).set(anyString(), anyString(), eq(TTL));
        assertDoesNotThrow(() -> cache.put(response));
    }

    @Test
    void shouldEvictMalformedJsonAndFallBack() {
        var response = response();
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(PREFIX + response.id())).thenReturn("{broken-json");
        assertEquals(Optional.empty(), cache.get(response.id()).response());
        verify(redis).delete(PREFIX + response.id());
    }

    @Test
    void shouldRejectDifferentIdEvenWithValidJson() throws Exception {
        var response = response();
        var mismatched = new LoanApplicationResponse(
                UUID.randomUUID(), response.customerId(), response.amount(),
                response.term(), response.currency(), response.status(),
                response.createdAt(), response.updatedAt());
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(PREFIX + response.id())).thenReturn(mapper.writeValueAsString(mismatched));
        assertEquals(Optional.empty(), cache.get(response.id()).response());
        verify(redis).delete(PREFIX + response.id());
    }

    @Test
    void shouldNotTouchRedisWhenDisabled() {
        var disabled = new LoanApplicationCache(redis, mapper,
                new LoanApplicationCacheProperties(false, TTL, PREFIX), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        assertEquals(Optional.empty(), disabled.get(UUID.randomUUID()).response());
        disabled.put(response());
        disabled.evict(UUID.randomUUID());
        verifyNoInteractions(redis);
    }

    @Test
    void shouldRejectInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> new LoanApplicationCacheProperties(true, Duration.ZERO, PREFIX));
        assertThrows(IllegalArgumentException.class,
                () -> new LoanApplicationCacheProperties(true, TTL, " "));
    }

    private LoanApplicationResponse response() {
        return new LoanApplicationResponse(
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                new BigDecimal("123456789.12"), 24, "EUR",
                LoanApplicationStatus.CREATED,
                Instant.parse("2026-10-12T10:15:30Z"),
                Instant.parse("2026-10-12T10:15:30Z")
        );
    }
}
