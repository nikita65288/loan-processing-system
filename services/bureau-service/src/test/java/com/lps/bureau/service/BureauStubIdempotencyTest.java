package com.lps.bureau.service;

import com.lps.contracts.command.PerformBureauCheckCommand;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Stateless retries compute the same logical event identity and business result. */
class BureauStubIdempotencyTest {
    @Test
    void retryKeepsResultIdentity() {
        var stub = new BureauStub();
        UUID id = UUID.randomUUID();
        var command = new PerformBureauCheckCommand(id, "PerformBureauCheck", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        var first = stub.check(command);
        var again = stub.check(command);
        assertEquals(first.eventId(), again.eventId());
        assertEquals(first.applicationId(), again.applicationId());
    }
}
