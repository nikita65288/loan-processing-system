package com.lps.creditscoring.service;

import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CreditScoringStubTest {
    @Test
    void shouldReturnDeterministicApprovedScore() {
        UUID applicationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        PerformCreditScoringCommand command = new PerformCreditScoringCommand(UUID.randomUUID(), "PerformCreditScoring", Instant.now(),
                correlationId, applicationId, UUID.randomUUID(), BigDecimal.TEN, 12, "EUR");

        CreditScoringCompletedEvent result = new CreditScoringStub().score(command);

        assertEquals(applicationId, result.applicationId());
        assertEquals(correlationId, result.correlationId());
        assertEquals(750, result.score());
        assertTrue(result.approved());
        assertNotEquals(command.commandId(), result.eventId());
    }
}
