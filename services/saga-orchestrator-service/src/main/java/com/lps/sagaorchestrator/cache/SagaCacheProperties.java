package com.lps.sagaorchestrator.cache;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Configuration for the optional read-only Saga snapshot cache. */
@ConfigurationProperties(prefix = "lps.cache.saga")
public record SagaCacheProperties(boolean enabled, Duration ttl, String keyPrefix) {
    public SagaCacheProperties {
        if (ttl == null || ttl.isNegative() || ttl.isZero())
            throw new IllegalArgumentException("Saga cache TTL must be positive");
        if (keyPrefix == null || keyPrefix.isBlank())
            throw new IllegalArgumentException("Saga cache prefix must be nonblank");
    }
}
