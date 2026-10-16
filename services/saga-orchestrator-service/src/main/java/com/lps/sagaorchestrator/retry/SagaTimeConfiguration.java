package com.lps.sagaorchestrator.retry;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** Shared time source for deterministic tests and restart-safe deadline calculations. */
@Configuration
@EnableConfigurationProperties(SagaRetryProperties.class)
public class SagaTimeConfiguration {
    @Bean
    Clock sagaClock() {
        return Clock.systemUTC();
    }
}
