package com.lps.bureau.service;

import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BureauStubTest {
    @Test
    void shouldReturnDeterministicApprovedResult() {
        UUID applicationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        PerformBureauCheckCommand command = new PerformBureauCheckCommand(UUID.randomUUID(), "PerformBureauCheck", Instant.now(),
                correlationId, applicationId, UUID.randomUUID());

        BureauCheckCompletedEvent result = new BureauStub().check(command);

        assertEquals(applicationId, result.applicationId());
        assertEquals(correlationId, result.correlationId());
        assertEquals(720, result.bureauScore());
        assertFalse(result.hasDelinquencies());
        assertTrue(result.approved());
        assertNotEquals(command.commandId(), result.eventId());
    }
}
