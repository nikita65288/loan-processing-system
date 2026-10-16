package com.lps.sagaorchestrator;

import com.lps.sagaorchestrator.cache.SagaCacheProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Application boundary for the owner of the durable loan-processing workflow. */
@SpringBootApplication
@EnableConfigurationProperties(SagaCacheProperties.class)
public class SagaOrchestratorServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(SagaOrchestratorServiceApplication.class, args);
    }
}
