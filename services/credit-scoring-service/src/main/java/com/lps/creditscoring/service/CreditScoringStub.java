package com.lps.creditscoring.service;

import com.lps.contracts.command.PerformCreditScoringCommand;
import com.lps.contracts.event.CreditScoringCompletedEvent;
import com.lps.contracts.MessageIdentity;
import org.springframework.stereotype.Service;

import java.time.Instant;

/** Deterministic scoring simulation used to validate Kafka contracts and service boundaries. */
@Service
public class CreditScoringStub {
    public CreditScoringCompletedEvent score(PerformCreditScoringCommand command) {
        return new CreditScoringCompletedEvent(MessageIdentity.resultEventId(command.commandId(), "CreditScoringCompleted"), "CreditScoringCompleted", Instant.now(),
                command.correlationId(), command.applicationId(), 750, true);
    }
}
