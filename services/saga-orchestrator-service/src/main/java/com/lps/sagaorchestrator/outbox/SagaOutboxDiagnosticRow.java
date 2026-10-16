package com.lps.sagaorchestrator.outbox;

import java.util.UUID;
import java.time.Instant;

/** Narrow read-only view: inspecting a Saga never loads Kafka command payloads. */
public interface SagaOutboxDiagnosticRow {
    UUID getId();
    UUID getApplicationId();
    String getCommandType();
    SagaOutboxStatus getStatus();
    Instant getCreatedAt();
}
