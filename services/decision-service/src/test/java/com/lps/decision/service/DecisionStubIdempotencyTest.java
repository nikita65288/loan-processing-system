package com.lps.decision.service;

import com.lps.contracts.command.MakeDecisionCommand;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Stateless retries compute the same logical event identity and business result. */
class DecisionStubIdempotencyTest {
    @Test
    void retryKeepsResultIdentity() {
        var stub = new DecisionStub();
        UUID id = UUID.randomUUID();
        var command = new MakeDecisionCommand(id, "MakeDecision", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), 750, 720, false);
        var first = stub.decide(command);
        var again = stub.decide(command);
        assertEquals(first.eventId(), again.eventId());
        assertEquals(first.applicationId(), again.applicationId());
    }
}
