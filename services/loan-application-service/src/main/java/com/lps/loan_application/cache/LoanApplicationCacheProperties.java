package com.lps.loan_application.cache;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Defines the small, locally configurable cache policy for loan application reads. */
@ConfigurationProperties(prefix = "lps.cache.loan-application")
public record LoanApplicationCacheProperties(
        boolean enabled,
        Duration ttl,
        String keyPrefix
) {
    public LoanApplicationCacheProperties {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("Loan application cache TTL must be positive");
        }
        if (keyPrefix == null || keyPrefix.isBlank()) {
            throw new IllegalArgumentException("Loan application cache key prefix must not be blank");
        }
    }
}
