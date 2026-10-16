package com.lps.sagaorchestrator.retry;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.sagaorchestrator.saga.SagaInstance;
import com.lps.sagaorchestrator.saga.SagaStep;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Fast deterministic checks: new physical sends must keep the same logical operation. */
class SagaStepAttemptTest {
    private static final SagaRetryProperties POLICY = new SagaRetryProperties(
            new SagaRetryProperties.Timeout(true, Duration.ofSeconds(30), Duration.ofSeconds(45), Duration.ofSeconds(30)),
            new SagaRetryProperties.Retry(3, Duration.ofSeconds(5)),
            new SagaRetryProperties.Scheduler(50));

    @Test
    void deadlineStartsAfterPublicationAndSurvivesAsStoredTimestamps() {
        Instant created = Instant.parse("2026-10-12T09:00:00Z");
        var saga = SagaInstance.start(new ApplicationCreatedEvent(
                UUID.randomUUID(), "ApplicationCreated", created, UUID.randomUUID(),
                UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("5000"), 12, "EUR"), created);
        UUID logicalCommandId = UUID.randomUUID();
        var first = SagaStepAttempt.initial(saga, SagaStep.BUREAU, logicalCommandId,
                "PerformBureauCheck", "loan.bureau.check.requested", "{\"commandId\":\"abc\"}", created, POLICY);
        assertNull(first.getDeadlineAt());
        assertNull(first.getPublishedAt());
        first.markPublished(created.plusSeconds(100));
        assertEquals(created.plusSeconds(145), first.getDeadlineAt());
        assertEquals(created.plusSeconds(150), first.getRetryAfterAt());
        UUID physicalId = UUID.randomUUID();
        var second = SagaStepAttempt.retry(first, physicalId, created.plusSeconds(151));
        assertEquals(2, second.getAttemptNumber());
        assertEquals(logicalCommandId, second.getCommandId());
        assertNotEquals(first.getOutboxEventId(), second.getOutboxEventId());
        assertEquals(first.getPayload(), second.getPayload());
        assertNull(second.getPublishedAt());
    }
}
