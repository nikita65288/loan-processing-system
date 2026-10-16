package com.lps.sagaorchestrator.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Allows deterministic integration tests to disable Outbox polling. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(
        name = "lps.scheduling.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class SchedulingConfiguration {
}
