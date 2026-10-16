package com.lps.creditscoring.service;

import com.lps.contracts.command.PerformCreditScoringCommand;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Stateless retries compute the same logical event identity and business result. */
class CreditScoringStubIdempotencyTest {
    @Test
    void retryKeepsResultIdentity() {
        var stub = new CreditScoringStub();
        UUID id = UUID.randomUUID();
        var command = new PerformCreditScoringCommand(id, "PerformCreditScoring", Instant.now(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), BigDecimal.TEN, 12, "EUR");
        var first = stub.score(command);
        var again = stub.score(command);
        assertEquals(first.eventId(), again.eventId());
        assertEquals(first.applicationId(), again.applicationId());
    }
}
