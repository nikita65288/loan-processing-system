package com.lps.decision.service;

import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.model.Decision;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DecisionStubTest {
    @Test
    void shouldApproveWhenAllDeterministicRulesPass() {
        MakeDecisionCommand command = command(750, 720, false);
        DecisionCompletedEvent result = new DecisionStub().decide(command);
        assertEquals(Decision.APPROVED, result.decision());
        assertEquals("PASS", result.reason());
    }

    @Test
    void shouldRejectWhenCreditScoreIsBelowThreshold() {
        MakeDecisionCommand command = command(650, 720, false);
        DecisionCompletedEvent result = new DecisionStub().decide(command);
        assertEquals(Decision.REJECTED, result.decision());
        assertEquals("FAIL_RULES", result.reason());
    }

    @Test
    void shouldRejectWhenBureauScoreIsBelowThreshold() {
        MakeDecisionCommand command = command(750, 650, false);
        DecisionCompletedEvent result = new DecisionStub().decide(command);
        assertEquals(Decision.REJECTED, result.decision());
    }

    @Test
    void shouldRejectWhenDelinquenciesExist() {
        MakeDecisionCommand command = command(750, 720, true);
        DecisionCompletedEvent result = new DecisionStub().decide(command);
        assertEquals(Decision.REJECTED, result.decision());
    }

    private MakeDecisionCommand command(int creditScore, int bureauScore, boolean delinquencies) {
        return new MakeDecisionCommand(UUID.randomUUID(), "MakeDecision", Instant.now(), UUID.randomUUID(),
                UUID.randomUUID(), creditScore, bureauScore, delinquencies);
    }
}
