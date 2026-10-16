package com.lps.sagaorchestrator.saga;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.model.Decision;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** No fake compensation commands exist for pure computation/read-only steps. */
class SagaCompensationPolicyTest {
    private final SagaCompensationPolicy policy = new SagaCompensationPolicy();

    @Test
    void everyCurrentStateRequiresNoCompensation() {
        var now = Instant.now();
        var saga = SagaInstance.start(new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", now,
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("8000"), 12, "EUR"), now);
        assertFalse(policy.requiresCompensation(saga));
        saga.completeScoring(750, saga.getCorrelationId(), now);
        assertFalse(policy.requiresCompensation(saga));
        saga.completeBureau(720, false, now);
        assertFalse(policy.requiresCompensation(saga));
        saga.completeDecision(Decision.REJECTED, "FAIL_RULES", now);
        assertFalse(policy.requiresCompensation(saga));
    }
}
