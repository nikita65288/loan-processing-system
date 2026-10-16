package com.lps.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.ApplicationCreatedEvent;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.model.Decision;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ContractSerializationTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void shouldRoundTripAllContracts() throws Exception {
        UUID applicationId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Instant now = Instant.now();

        assertEquals(applicationId, mapper.readValue(mapper.writeValueAsString(
                new ApplicationCreatedEvent(UUID.randomUUID(), "ApplicationCreated", now, correlationId,
                        applicationId, UUID.randomUUID(), BigDecimal.TEN, 12, "EUR")), ApplicationCreatedEvent.class).applicationId());
        assertEquals(applicationId, mapper.readValue(mapper.writeValueAsString(
                new PerformCreditScoringCommand(UUID.randomUUID(), "PerformCreditScoring", now, correlationId,
                        applicationId, UUID.randomUUID(), BigDecimal.TEN, 12, "EUR")), PerformCreditScoringCommand.class).applicationId());
        assertEquals(applicationId, mapper.readValue(mapper.writeValueAsString(
                new CreditScoringCompletedEvent(UUID.randomUUID(), "CreditScoringCompleted", now, correlationId,
                        applicationId, 750, true)), CreditScoringCompletedEvent.class).applicationId());
        assertEquals(applicationId, mapper.readValue(mapper.writeValueAsString(
                new PerformBureauCheckCommand(UUID.randomUUID(), "PerformBureauCheck", now, correlationId,
                        applicationId, UUID.randomUUID())), PerformBureauCheckCommand.class).applicationId());
        assertEquals(applicationId, mapper.readValue(mapper.writeValueAsString(
                new BureauCheckCompletedEvent(UUID.randomUUID(), "BureauCheckCompleted", now, correlationId,
                        applicationId, 720, false, true)), BureauCheckCompletedEvent.class).applicationId());
        assertEquals(applicationId, mapper.readValue(mapper.writeValueAsString(
                new MakeDecisionCommand(UUID.randomUUID(), "MakeDecision", now, correlationId,
                        applicationId, 750, 720, false)), MakeDecisionCommand.class).applicationId());
        assertEquals(Decision.APPROVED, mapper.readValue(mapper.writeValueAsString(
                new DecisionCompletedEvent(UUID.randomUUID(), "DecisionCompleted", now, correlationId,
                        applicationId, Decision.APPROVED, "PASS")), DecisionCompletedEvent.class).decision());
    }
}
