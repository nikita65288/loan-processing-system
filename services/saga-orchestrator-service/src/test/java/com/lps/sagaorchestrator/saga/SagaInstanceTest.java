package com.lps.sagaorchestrator.saga;

import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.model.Decision;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests document the entity's allowed transitions without Spring or Kafka. */
class SagaInstanceTest {
    @Test
    void happyPathStoresContextAndCompletesEvenWhenRejected() {
        Instant time = Instant.now();
        SagaInstance saga = saga();
        saga.completeScoring(745, UUID.randomUUID(), time);
        assertEquals(SagaStep.BUREAU, saga.getCurrentStep());
        assertEquals(745, saga.getCreditScore());

        saga.completeBureau(690, true, time.plusSeconds(1));
        assertEquals(SagaStep.DECISION, saga.getCurrentStep());
        assertEquals(690, saga.getBureauScore());
        assertTrue(saga.getHasDelinquencies());
        saga.completeDecision(Decision.REJECTED, "FAIL_RULES", time.plusSeconds(2));
        assertEquals(SagaStep.NOTIFICATION, saga.getCurrentStep());
        assertEquals(SagaStatus.IN_PROGRESS, saga.getStatus());
        assertEquals(Decision.REJECTED, saga.getFinalDecision());
        assertEquals("FAIL_RULES", saga.getDecisionReason());
        assertNull(saga.getCompletedAt());
        saga.completeNotification(time.plusSeconds(3));
        assertEquals(SagaStatus.COMPLETED,saga.getStatus());
        assertNotNull(saga.getCompletedAt());
    }

    @Test
    void onlyNextTransitionIsAllowed() {
        SagaInstance saga = saga();
        assertThrows(IllegalStateException.class, () -> saga.completeBureau(720, false, Instant.now()));
        assertThrows(IllegalStateException.class,
                () -> saga.completeDecision(Decision.APPROVED, "PASS", Instant.now()));
        assertEquals(SagaStep.SCORING, saga.getCurrentStep());
        saga.completeScoring(740, null, Instant.now());
        assertThrows(IllegalStateException.class, () -> saga.completeScoring(690, null, Instant.now()));
        assertEquals(SagaStep.BUREAU, saga.getCurrentStep());
    }

    @Test
    void missingScoreIsRejectedBeforeAdvancing() {
        SagaInstance saga = saga();
        assertThrows(NullPointerException.class, () -> saga.completeScoring(null, null, Instant.now()));
        assertEquals(SagaStep.SCORING, saga.getCurrentStep());
        saga.completeScoring(700, null, Instant.now());
        assertThrows(NullPointerException.class, () -> saga.completeBureau(null, false, Instant.now()));
        assertEquals(SagaStep.BUREAU, saga.getCurrentStep());
        assertThrows(IllegalStateException.class, () -> saga.completeDecision(Decision.REJECTED, null, Instant.now()));
    }

    private SagaInstance saga() {
        return SagaInstance.start(new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated",
                Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("10000.00"), 12, "EUR"), Instant.now());
    }
}
