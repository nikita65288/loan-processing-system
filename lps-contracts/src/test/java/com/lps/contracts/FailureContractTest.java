package com.lps.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lps.contracts.event.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** The failure contracts are explicit JSON records with stable logical event IDs. */
class FailureContractTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void eachFailureTypeRoundTripsWithoutSpringKafkaHeaders() throws Exception {
        UUID id = UUID.randomUUID();
        UUID app = UUID.randomUUID();
        UUID correlation = UUID.randomUUID();
        var at = Instant.parse("2026-10-12T10:00:00Z");
        var scoring = new CreditScoringFailedEvent(MessageIdentity.resultEventId(id, "CreditScoringFailed"),
                "CreditScoringFailed", at, correlation, app, id, "SCORING_INVALID_REQUEST", false);
        var bureau = new BureauCheckFailedEvent(MessageIdentity.resultEventId(id, "BureauCheckFailed"),
                "BureauCheckFailed", at, correlation, app, id, "BUREAU_TEMPORARILY_UNAVAILABLE", true);
        var decision = new DecisionFailedEvent(MessageIdentity.resultEventId(id, "DecisionFailed"),
                "DecisionFailed", at, correlation, app, id, "DECISION_INVALID_REQUEST", false);
        assertEquals(scoring, mapper.readValue(mapper.writeValueAsString(scoring), CreditScoringFailedEvent.class));
        assertEquals(bureau, mapper.readValue(mapper.writeValueAsString(bureau), BureauCheckFailedEvent.class));
        assertEquals(decision, mapper.readValue(mapper.writeValueAsString(decision), DecisionFailedEvent.class));
        assertNotEquals(scoring.eventId(), bureau.eventId());
        assertEquals(bureau.eventId(), MessageIdentity.resultEventId(id, "BureauCheckFailed"));
    }
}
