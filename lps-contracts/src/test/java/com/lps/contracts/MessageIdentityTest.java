package com.lps.contracts;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** Command retries preserve logical event identity without persistent state. */
class MessageIdentityTest {
    @Test
    void repeatOfCommandKeepsIdButEventTypeAndCommandChangeIt() {
        UUID command = UUID.randomUUID();
        UUID first = MessageIdentity.resultEventId(command, "CreditScoringCompleted");
        assertEquals(first, MessageIdentity.resultEventId(command, "CreditScoringCompleted"));
        assertNotEquals(first, MessageIdentity.resultEventId(UUID.randomUUID(), "CreditScoringCompleted"));
        assertNotEquals(first, MessageIdentity.resultEventId(command, "BureauCheckCompleted"));
        assertNotEquals(first, MessageIdentity.resultEventId(command, "DecisionCompleted"));
    }
}
