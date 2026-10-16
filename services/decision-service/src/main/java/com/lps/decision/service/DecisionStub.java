package com.lps.decision.service;

import com.lps.contracts.command.MakeDecisionCommand;
import com.lps.contracts.event.DecisionCompletedEvent;
import com.lps.contracts.model.Decision;
import com.lps.contracts.MessageIdentity;
import org.springframework.stereotype.Service;

import java.time.Instant;

/** Deterministic decision rule; this is not a production decision engine. */
@Service
public class DecisionStub {
    public DecisionCompletedEvent decide(MakeDecisionCommand command) {
        boolean approved = command.creditScore() >= 700
                && command.bureauScore() >= 700
                && !command.hasDelinquencies();
        Decision decision = approved ? Decision.APPROVED : Decision.REJECTED;
        String reason = approved ? "PASS" : "FAIL_RULES";
        return new DecisionCompletedEvent(
                MessageIdentity.resultEventId(command.commandId(), "DecisionCompleted"),
                "DecisionCompleted",
                Instant.now(),
                command.correlationId(),
                command.applicationId(),
                decision,
                reason
        );
    }
}
